const Promise = require('bluebird')
const path = require('path')
const fs = Promise.promisifyAll(require('fs'))
const child = Promise.promisifyAll(require('child_process'))
const EventEmitter = require('events')

const rimraf = require('rimraf')
const rimrafAsync = Promise.promisify(rimraf)
const mkdirp = require('mkdirp')
const mkdirpAsync = Promise.promisify(mkdirp)
const UUID = require('uuid')
const debug = require('debug')('Boot')
const deepEqual = require('fast-deep-equal')

const { isNonEmptyString } = require('../lib/assertion')
const DataStore = require('../lib/DataStore')
const Fruitmix = require('../fruitmix/Fruitmix')
const { probe, probeAsync, umountBlocksAsync } = require('./storage')
const { UdevMonitor, StorageUpdater } = require('./diskmon')

/**
Boot is the top-level container

@module Boot
*/
class State {
  constructor (ctx, ...args) {
    this.ctx = ctx
    this.ctx.state = this

    console.log('=== Boot entering ===', this.constructor.name)
    this.enter(...args)
    console.log('=== Boot entered ===', this.constructor.name)

    this.ctx.emit('StateEntered', this.constructor.name)
  }

  setState (State, ...args) {
    this.exit()
    new State(this.ctx, ...args)
  }

  enter () {
  }

  exit () {
    console.log('=== Boot exited ===', this.constructor.name)
  }

  boundUserUpdated () {
  }

  boundVolumeUpdated () {
  }

  storageUpdate() {}

  presetLoaded () {
  }

  continuable () {
    // for winas
    if (IS_WINAS) return true

    if (!this.ctx.boundUser) return false
    if (this.ctx.volumeStore.data === undefined) return false
    if (this.ctx.preset === undefined) return false
    return true
  }

  next () {
    // for embed volume
    if (IS_WINAS) return this.setState(EmbedVolumeCheck)

    if (this.ctx.bootable()) {
      if (this.ctx.preset && this.ctx.preset.state === 'PENDING') {
        this.setState(Presetting)
      } else {
        this.setState(Starting)
      }
    } else {
      this.setState(Unavailable)
    }
  }

  init (target, mode, callback) {
    process.nextTick(() => callback(new Error('invalid state')))
  }

  import (volumeUUID, callback) {
    process.nextTick(() => callback(new Error('invalid state')))
  }

  repair (devices, mode, callback)  {
    process.nextTick(() => callback(new Error('invalid state')))
  }

  add (devices, mode, callback) {
    process.nextTick(() => callback(new Error('invalid state')))
  }

  remove (devices, callback) {
    process.nextTick(() => callback(new Error('invalid state')))
  }

  uninstall (props, callback) {
    process.nextTick(() => callback(new Error('invalid state')))
  }

  format (target, callback){
    process.nextTick(() => callback(new Error('invalid state')))
  }

  // TODO this is a pure function, or maybe static
  createBoundVolume (storage, volume, boundVolumeId) {
    let devices = volume.devices.map(dev => {
      let blk = storage.blocks.find(blk => blk.name === dev.name)
      return {
        removable: blk.removable,
        size: blk.size,
        model: blk.model,
        serial: blk.serial,
        btrfsDevice: blk.btrfsDevice,
        idBus: blk.idBus
      }
    })

    // report boundVolume update, bootstrap to restart peerstar
    // TODO: bad here
    process.send && process.send(JSON.stringify({
      type: 'appifi_boundVolume_update',
      data: {}
    }))

    return {
      devices,
      label: volume.label,
      id: boundVolumeId || UUID.v4(),
      uuid: volume.uuid,
      total: volume.total,
      usage: {
        system: { mode: volume.usage.system.mode },
        metadata: { mode: volume.usage.metadata.mode },
        data: { mode: volume.usage.data.mode }
      }
    }
  }
}

/**
Failed
*/
class Failed extends State {
  enter (err) {
    this.err = err
  }
}

/**
Probing the storage
*/
class Probing extends State {
  enter () {
    probe(this.ctx.conf.storage, (err, storage) => {
      if (err) {
        this.setState(ProbeFailed, err)
      } else {
        this.ctx.storage = storage
        if (this.continuable()) {
          this.next()
        } else {
          this.setState(Pending)
        }
      }
    })
  }
}

/**
ProbeFailed
*/
class ProbeFailed extends State {
  enter (err) {
    this.err = err
    this.timer = setTimeout(() => this.setState(Probing), 10000)
    console.log('ProbeFailed', err)
  }

  exit () {
    clearTimeout(this.timer)
  }
}

/**
Pending is a joining state waiting for the following conditions are met.
1. the chassis-user binding is loaded or received, this rule is specific for phicomm device
2. the chassis-volume binding is loaded
3. preset is loaded
*/
class Pending extends State {
  enter () {
  }

  boundUserUpdated () {
    if (this.continuable()) this.next()
  }

  volumeUpdated () {
    if (this.continuable()) this.next()
  }

  presetUpdated () {
    if (this.continuable()) this.next()
  }
}

class Presetting extends State {
  enter () {

  }
}

class EmbedVolumeCheck extends State {
  enter() {
    let volume = this.ctx.storage.blocks.find(v => v.isUSB)
    if (!volume) { // VolumeNotFound
      return process.nextTick(() => this.setState(EmbedVolumeFailed,
        Object.assign(new Error('volume not found'), { code: 'EVOLUMENOTFOUND'})))
    }
    if (!volume.isBtrfs) {
      return process.nextTick(() => this.setState(EmbedVolumeFailed,
        Object.assign(new Error('volume format error'), { code: 'EVOLUMEFORMAT'})))
    }
    if (volume.isMissing) {
      return process.nextTick(() => this.setState(EmbedVolumeFailed,
        Object.assign(new Error('volume missing'), { code: 'EVOLUMEMISS'})))
    }
    // validate uses.json && drives.json
    let fruitmixDir = path.join(volume.mountpoint, this.ctx.conf.storage.fruitmixDir)
    fs.exists(fruitmixDir, exists => {
      if (!exists) { // safe
        this.setState(Starting, volume)
      } else {
        // check users.json
        this.validateUserFile(fruitmixDir, err => {
          if (err) {
            return process.nextTick(() => this.setState(EmbedVolumeFailed,
              Object.assign(new Error('volume has users.json but data parse error'), { code: 'EVOLUMEFILE'})))
          } else {
            // check drives.json
            this.validateDriveFile(fruitmixDir, err => {
              if (err) {
                return process.nextTick(() => this.setState(EmbedVolumeFailed,
                  Object.assign(new Error('volume has drives.json but data parse error'), { code: 'EVOLUMEFILE'})))
              } else { // all passed
                this.setState(Starting, volume)
              }
            })
          }
        })
      }
    })
  }

  // vaildate users.json
  // if not exist / data validated safe
  validateUserFile(fdir, callback) {
    let ufile = path.join(fdir, 'users.json')
    fs.exists(ufile, exists => {
      if (!exists) return callback(null)
      try{
        let users = JSON.parse(fs.readFileSync(ufile))
        if (!Array.isArray(users)) throw new Error('users.json format error')
        let check = users.every(x => {
          let propertys = Object.getOwnPropertyNames(x)
          if (!propertys.includes('uuid') ||
            !propertys.includes('winasUserId') ||
            !propertys.includes('phoneNumber')) {
              return false
          }
          // TODO: define each user all properties         
          // return propertys.every(p => [].includes(p))
          return true
        })
        if (!check) throw new Error('users.json format error')
      } catch(e) {
        return callback(e)
      }
      return callback(null)
    })
  }

  validateDriveFile(fdir, callback) {
    let dfile = path.join(fdir, 'drives.json')
    fs.exists(dfile, exists => {
      if (!exists) return callback(null)
      try{
        let drives = JSON.parse(fs.readFileSync(dfile))
        if (!Array.isArray(drives)) throw new Error('drives.json format error')
        let check = drives.every(x => {
          let propertys = Object.getOwnPropertyNames(x)
          if (!propertys.includes('uuid') ||
            !propertys.includes('type')) {
              return false
          }
          // TODO: define each user all properties         
          // return propertys.every(p => [].includes(p))
          return true
        })
        if (!check) throw new Error('drives.json format error')
      } catch(e) {
        return callback(e)
      }
      return callback(null)
    })
  }
}

class EmbedVolumeFailed extends State {
  enter (err) {
    this.error = err
    console.log('EmbedVolumeFailed', err)
  }

  storageUpdate() {
    this.setState(Probing)
  }

  format(target, callback) {
    this.setState(EmbedVolumeInit, target, callback)
  }
}

// FIXME: merge to `Initializing` state
class EmbedVolumeInit extends State {
  enter(target, callback) {
    if (!target) { // find default target
      let volume = this.ctx.storage.blocks.find(v => v.isUSB)
      if (!volume) {
        callback('target not found')
        return this.setState(Probing)
      } else
        target = volume.name
    }
    this.initAsync(target)
      .then(_ => {
        console.log('init success, go to Probing')
        callback(null)
        this.setState(Probing)
      })
      .catch(e => {
        callback(e)
        this.setState(Probing)
      })
  }

  async initAsync(target) {
    let storage = await probeAsync(this.ctx.conf.storage)
    let block = storage.blocks.find(blk => blk.name === target)
    if (!block) throw new Error('target not found')
    if (!block.isDisk) throw new Error(`device ${target} is not a disk`)
    if (block.unformattable) throw new Error(`device ${target} is not formattable`)
    let devname = block.devname
    debug(`mkfs.btrfs single`, devname)

    // step 2: unmount
    await umountBlocksAsync(storage, [target])
    // step 3: mkfs
    await child.execAsync(`mkfs.btrfs -d single -f ${devname}`)
    // step 4: probe again
    storage = await probeAsync(this.ctx.conf.storage)

    block = storage.blocks.find(blk => blk.name === target)
    if (!block) throw new Error('cannot find a volume containing expected block name')

    let volume = storage.volumes.find(v => v.uuid === block.fileSystemUUID)
    if (!volume) throw new Error('cannot find a volume containing expected block name')

    // ensure bound volume data format
    if (!volume.usage || !volume.usage.system || !volume.usage.metadata || !volume.usage.data) {
      console.log(volume)
      throw new Error('volume usage not properly detected')
    }
  }
}

class Starting extends State {
  enter (v) {
    let opts, boundVolumeUUID, volume, fruitmixDir
    if (IS_WINAS) {
      volume = v
      fruitmixDir = path.join(v.mountpoint, this.ctx.conf.storage.fruitmixDir)
    } else { // for others
      boundVolumeUUID = this.ctx.volumeStore.data.uuid
      volume = this.ctx.storage.volumes.find(v => v.uuid === boundVolumeUUID)
      fruitmixDir = path.join(volume.mountpoint, this.ctx.conf.storage.fruitmixDir)
    }

    opts = Object.assign({}, this.ctx.fruitmixOpts, {
      fruitmixDir,
      boundVolume: this.ctx.volumeStore ? this.ctx.volumeStore.data : undefined,
      boundUser: this.ctx.boundUser,
      ejectHandler: this.ctx.ejectUSB.bind(this.ctx)
    })
    
    let fruitmix = new Fruitmix(opts)

    fruitmix.setStorage(this.ctx.storage)

    fruitmix.once('FruitmixStarted', () => this.setState(Started, fruitmix))
  }
}

/**
 * job defined:
 * {
 *  type: 'updateBoundVolume' // 'addDevice' / 'removeDevice' / 'updateBoundVolume'
 *  props: {
 *    ...
 *  }
 *  callback: 'function' // opt
 * }
 */
class Started extends State {
  enter (fruitmix) {
    this.ctx.fruitmix = fruitmix
    this.udevMonitor = new UdevMonitor()
    this.udevMonitor.on('update', () => this.ctx.storageUpdater.probe())
    
    if (!IS_WINAS) {
      this.jobs = []
      let job = {
        type: 'updateBoundVolume',
        props: {}
      }
      this.jobs.push(job)
      this.reqSchedJob()
      this.uninstalling = false
    } else {
      // if (this.ctx.boundUser) fruitmix.bindFirstUser(this.ctx.boundUser)
    }

    // start timer to do balance
    this.balanceTimer = setInterval(() => {
      child.exec(`btrfs balance start ${ fruitmix.fruitmixDir }`, err => {
        if (err) {
          child.exec(`btrfs balance start -dusage=0 ${ fruitmix.fruitmixDir }`, err => {
            console.log('balance error: ', err)
            if (err) {
              child.exec(`btrfs balance start -musage=0 ${ fruitmix.fruitmixDir }`, err => {
                console.log('balance error: ', err)
              })
            }
          })
        }
      })
    }, 24 * 1000 * 60 * 60)
  }

  exit () {
    this.ctx.fruitmix = null
    if (this.jobs) {
      let jobs = [...this.jobs]
      this.jobs = []
      jobs.forEach(j => j.callback && j.callback(new Error('exit started state')))
    }
    this.udevMonitor.destroy()
    clearInterval(this.balanceTimer)
  }

  add (devices, mode, callback) {
    if (this.uninstalling)
      return process.nextTick(() => callback(new Error('uninstalling')))
    if (this.jobs.length)
      return process.nextTick(() => callback(new Error('target busy')))
    this.setState(Adding, devices, mode, callback)
  }

  boundUserUpdated () {
    // FIXME
  }

  reqSchedJob () {
    if (this.jobScheduled) return
    this.jobScheduled = true
    process.nextTick(() => this.scheduleJob())
  }

  scheduleJob () {
    if (!this.workingJobs) this.workingJobs = []
    this.jobScheduled = false
    if (this.workingJobs.length) return
    while(this.workingJobs.length === 0 && this.jobs.length) {
      let job = this.jobs.shift()
      let doFunc
      switch (job.type) {
        case 'updateBoundVolume':
          doFunc = this.updateBoundVolumeAsync.bind(this)
          break
        case 'addDevice':
          doFunc = this.doAddAsync.bind(this)
          break
        case 'removeDevice':
          doFunc = this.doRemoveAsync.bind(this)
          break
        default:
          break
      }

      if (doFunc) {
        this.workingJobs.push(job)
        doFunc(job.props)
          .then(data => {
            console.log('boot started state pass job: ' + job.type)
            this.workingJobs.pop()
            if (job.callback) job.callback(null, data)
            this.reqSchedJob()
          })
          .catch(err => {
            console.log('boot started state fail job: ' + job.type)
            console.log(err)
            this.workingJobs.pop()
            if (job.callback) job.callback(err)
            this.reqSchedJob()
          })
      }
    }
  }

  async updateBoundVolumeAsync () {
    let boundVolume = this.ctx.volumeStore.data
    let volumeUUID = boundVolume.uuid
    let volume = this.ctx.storage.volumes.find(v => v.uuid === volumeUUID)

    // fix metadata single
    if (volume.usage.data.mode === 'single' && volume.devices.length === 1 && volume.usage.metadata.mode !== 'DUP') {
      await child.execAsync(`btrfs balance start -f -mconvert=dup ${ volume.mountpoint }`)
      let storage = await probeAsync(this.ctx.conf.storage)
      this.ctx.storage = storage
    }

    let newBoundVolume = this.createBoundVolume(this.ctx.storage, volume, boundVolume.id)
    return new Promise((resolve, reject) => {
      this.ctx.volumeStore.save(newBoundVolume, err =>
        err ? reject(err) : resolve(newBoundVolume))})
  }
  
  remove (devices, callback) {
    if (IS_WINAS) return process.nextTick(() => callback(new Error('Error Operation')))

    if (this.uninstalling) return callback(new Error('station in uninstalling'))
    if (!Array.isArray(devices) || devices.length !== 1) {
      return callback(new Error('devices must be an one item array'))
    }

    let job = {
      type: 'removeDevice',
      props: {
        devices
      },
      callback
    }

    this.jobs.push(job)
    this.reqSchedJob()
  }

  async doRemoveAsync ({ devices }) {
    let wantD = devices[0]
    let volumeUUID = this.ctx.volumeStore.data.uuid
    let volume = this.ctx.storage.volumes.find(v => v.uuid === volumeUUID)
    if (volume.devices.length !== 2) throw new Error('volume only has one device')

    let waitD = volume.devices.find(d => d.name === wantD.name)
    if (!waitD) throw new Error('device not found in volume')

    await child.execAsync(`btrfs balance start -f -mconvert=single -dconvert=single ${ volume.mountpoint }`)
    await child.execAsync(`btrfs device delete ${ waitD.path } ${ volume.mountpoint }`)
    await child.execAsync(`btrfs balance start -f -mconvert=dup ${ volume.mountpoint }`)

    let storage = await probeAsync(this.ctx.conf.storage)

    this.ctx.storage = storage

    return await this.updateBoundVolumeAsync()
  }

  uninstall(props, cb) {
    
    if (IS_WINAS) return process.nextTick(() => cb(new Error('Error Operation')))

    let callback = (err) => {
      this.uninstalling = false
      console.log(err)
      cb(err)
    }
    if (this.uninstalling) return callback(new Error('station in uninstalling'))
    if (this.jobs.length) {
      return callback(new Error('station busy'))
    }
    if (props.format && typeof props.format !== 'boolean') {
      return callback(Object.assign(new Error('props error'), { status: 400 }))
    }
    this.uninstalling = true
    let boundVolumeUUID = this.ctx.volumeStore.data.uuid
    this.ctx.volumeStore.save(null, (err, data) => {
      if (err) return callback(err)
      if (props.format) {
        let volume = this.ctx.storage.volumes.find(v => v.uuid === boundVolumeUUID)
        let fruitmixDir = path.join(volume.mountpoint, this.ctx.conf.storage.fruitmixDir)
        rimraf(fruitmixDir, err => {
          this.uninstalling = false
          if (err) cb(err)
          else cb(null)
          if (props.reset) {
            console.log('========================')
            console.log('uninstall success')
            console.log('reset success')
            console.log('reboot')
            console.log('========================')
            return child.exec('reboot', err => {
              if (err) console.log(err)
            })
          }
          else process.exit(61)
        })
      } else {
        callback(null)
      }
    })
  }
}

class Unavailable extends State {
  init (target, mode, callback) {
    let storage = this.ctx.storage
    if (!storage) return process.nextTick(() => callback(new Error('storage not available')))

    if (['single', 'raid1'].indexOf(mode) === -1) {
      return process.nextTick(() => callback(new Error('invalid mode')))
    }

    // target must be non-empty string array with sd? pattern
    if (!Array.isArray(target) ||
      target.length === 0 ||
      !target.every(name => typeof name === 'string') ||
      !target.every(name => /^sd[a-z]$/.test(name))) {
      return process.nextTick(() => callback(new Error('invalid target names')))
    }

    // undup and sort
    let target2 = Array.from(new Set(target)).sort()
    this.setState(Initializing, target2, mode, callback)
  }

  import (volumeUUID, callback) {
    this.setState(Importing, volumeUUID, callback)
  }

  repair (devices, mode, callback) {
    this.setState(Repairing, devices, mode, callback)
  }

  add (devices, mode, callback) {
    this.setState(Adding, devices, mode, callback)
  }
}

class Adding extends State {
  enter (devices, mode, callback) {
    this.doAddAsync(devices, mode)
      .then(boundVolume => {
        console.log('add success, go to Probing')
        this.setState(Probing)
        callback(null, boundVolume)
      })
      .catch(err => {0
        console.log('add failed, go to Probing')
        this.setState(Probing)
        callback(err)
      })
  }

  async doAddAsync (devices, mode) {
    let storage = await probeAsync(this.ctx.conf.storage)
    // can only add one device
    let wantD = devices[0]
    let volumeUUID = this.ctx.volumeStore.data.uuid
    let volume = this.ctx.storage.volumes.find(v => v.uuid === volumeUUID)
    let block = this.ctx.storage.blocks.find(b => b.name === wantD.name)

    if (!block) throw new Error('block not found')
    if (!volume) throw new Error('volume not found')
    if (volume.devices.length !== 1) throw new Error('volume has more then one device')
    if (volume.devices.find(d => d.name === wantD.name)) throw new Error('device has already in volume')
    await umountBlocksAsync(storage, [ wantD.name ])
    await child.execAsync(`btrfs device add -f ${ block.devname } ${ volume.mountpoint}`)

    if (mode === 'single') {
      await child.execAsync(`btrfs balance start -f -mconvert=raid1 ${ volume.mountpoint }`)
    } else {
      await child.execAsync(`btrfs balance start -f -dconvert=raid1 -mconvert=raid1 ${ volume.mountpoint }`)
    }

    storage = await probeAsync(this.ctx.conf.storage)

    this.ctx.storage = storage

    return await this.updateBoundVolumeAsync()
  }

  async updateBoundVolumeAsync () {
    let boundVolume = this.ctx.volumeStore.data
    let volumeUUID = boundVolume.uuid
    let volume = this.ctx.storage.volumes.find(v => v.uuid === volumeUUID)

    // fix metadata single
    if (volume.usage.data.mode === 'single' && volume.devices.length === 1 && volume.usage.metadata.mode !== 'DUP') {
      await child.execAsync(`btrfs balance start -f -mconvert=dup ${ volume.mountpoint }`)
      let storage = await probeAsync(this.ctx.conf.storage)
      this.ctx.storage = storage
    }

    let newBoundVolume = this.createBoundVolume(this.ctx.storage, volume, boundVolume.id)
    return new Promise((resolve, reject) => {
      this.ctx.volumeStore.save(newBoundVolume, err =>
        err ? reject(err) : resolve(newBoundVolume))})
  }
}

/**
  for wisnuc legacy, single '/etc/wisnuc.json' file is used.
  for wisnuc/phicomm
    <chassisDir>    // located on emmc
      user.json     // single file in json format, maintained by bootstrap, not appifi; for wisnuc, this file
                    // does NOT exist
      volume        // single file containing volume UUID
      <volumeUUID>
        storage.json
        users.json
        drives.json
        tags.json

  for tmp
    <chassisDir>
      atmp          // used by appifi
      btmp          // used by bootstrap
*/

/**
  `Initialization` is an exclusive process to create the bound volume through the following steps:

  1. probe and get the latest storage status.
  2. umount devices
  3. mkfs.btrfs
  4. probe again (this is because there is a bug in mkfs.btrfs output and the output format changes)
  5. find newly created volume containing given devices
  6. create users.json in a temporary directory and then rename the directory to prevent leaving an empty fruitmix dir.
  7. save volume information into store.

  The callers shall validate arguments before state transition.
*/
class Initializing extends State {
  // target should be valid!
  enter (target, mode, callback) {
    this.initAsync(target, mode)
      .then(boundVolume => {
        console.log('init success, go to Probing')
        this.setState(Probing)
        callback(null, boundVolume)
      })
      .catch(e => {
        this.setState(Probing)
        callback(e)
      })
  }

  async initAsync (target, mode) {
    let storage, blocks
    let devnames = []

    // step 1: probe
    storage = await probeAsync(this.ctx.conf.storage)

    // target name and translate to devname (devpath acturally)
    for (let i = 0; i < target.length; i++) {
      let block = storage.blocks.find(blk => blk.name === target[i])
      if (!block) throw new Error(`device ${target[i]} not found`)
      if (!block.isDisk) throw new Error(`device ${target[i]} is not a disk`)
      if (block.unformattable) throw new Error(`device ${target[i]} is not formattable`)
      devnames.push(block.devname)
    }

    debug(`mkfs.btrfs ${mode}`, devnames)

    // step 2: unmount
    await umountBlocksAsync(storage, target)

    // step 3: mkfs
    await child.execAsync(`mkfs.btrfs -d ${mode} -f ${devnames.join(' ')}`)

    // step 4: probe again
    storage = await probeAsync(this.ctx.conf.storage)

    let block = storage.blocks.find(blk => blk.name === target[0])
    if (!block) throw new Error('cannot find a volume containing expected block name')

    let volume = storage.volumes.find(v => v.uuid === block.fileSystemUUID)
    if (!volume) throw new Error('cannot find a volume containing expected block name')

    // ensure bound volume data format
    if (!volume.usage || !volume.usage.system || !volume.usage.metadata || !volume.usage.data) {
      console.log(volume)
      throw new Error('volume usage not properly detected')
    }

    let mp = volume.mountpoint
    let fruitmixDir = this.ctx.conf.storage.fruitmixDir

    // replacing the top dirname, such as <mp>/<uuid>/fruitmix or <mp>/<uuid>
    let rand = UUID.v4()
    let tmpDir = path.join(mp, rand, ...fruitmixDir.split(path.sep).slice(1))

    // such as <mp>/<uuid>
    let src = path.join(mp, rand)
    // such as <mp>/wisnuc or <mp>/phicomm
    let dst = path.join(mp, fruitmixDir.split(path.sep)[0])

    let users = [{
      uuid: UUID.v4(),
      username: this.ctx.boundUser.username,
      isFirstUser: true,
      isAdmin: true,
      phicommUserId: this.ctx.boundUser.phicommUserId, // for phi
      winasUserId: this.ctx.boundUser.id,
      password: this.ctx.boundUser.password,
      status: 'ACTIVE',
      createTime: new Date().getTime(),
      lastChangeTime: new Date().getTime(),
      phoneNumber: this.ctx.boundUser.username
    }]

    await mkdirpAsync(tmpDir)
    await fs.writeFileAsync(path.join(tmpDir, 'users.json'), JSON.stringify(users, null, '  '))
    await fs.renameAsync(src, dst)

    let boundVolume = this.createBoundVolume(storage, volume)

    return new Promise((resolve, reject) => {
      this.ctx.volumeStore.save(boundVolume, err => {
        if (err) {
          reject(err)
        } else {
          resolve(boundVolume)
        }
      })
    })
  }
}

/**
for importing an existing volume
*/
class Importing extends State {
  // volumeUUID should be valid!
  enter (volumeUUID, callback) {
    let storage, volume
    try {
      storage = this.ctx.storage
      if (!storage) throw new Error('storage not available')
      if (!storage.volumes || !Array.isArray(storage.volumes) || storage.volumes.length === 0) throw new Error('storage.volumes not available')
      volume = storage.volumes.find(v => v.uuid === volumeUUID)
      if (!volume) throw new Error('volume not found')

      if (volume.isMissing) throw new Error('volume is missing')

      let supportMode = ['single', 'raid1']
      if (!volume.usage || !volume.usage.data || typeof volume.usage.data.mode !== 'string') throw new Error('volume usage error')

      if (!supportMode.includes(volume.usage.data.mode.toLowerCase())) throw new Error('volume mode not support')

      if (!Array.isArray(volume.users)) throw new Error('volume users not found')

      let firstUser = volume.users.find(u => u.isFirstUser === true)
      if (!firstUser) throw new Error('volume admin not found')

      if (firstUser.winasUserId !== this.ctx.boundUser.winasUserId) throw new Error('volume admin <-> boundUser mismatch')
    } catch (e) {
      return process.nextTick(() => {
        this.setState(Probing)
        callback(e)
      })
    }

    let boundVolume = this.createBoundVolume(storage, volume)
    this.ctx.volumeStore.save(boundVolume, err => {
      this.setState(Probing)
      if (err) return callback(err)
      return callback(null, boundVolume)
    })
  }
}

/**
for repairing a broken volume
*/
class Repairing extends State {
  enter (devices, mode, callback) {
    this.repairAsync(devices, mode)
      .then(data => {
        console.log('repair success, go to Probing')
        this.setState(Probing)
        callback(null, data)
      })
      .catch(e => {
        console.log('repair failed, ', e)
        this.setState(Probing)
        callback(e)
      })
  }

  async repairAsync (devs, mode) {
    let supportMode = ['single', 'raid1']
    if (supportMode.indexOf(mode) === -1) throw new Error('mode error')

    // verify devices and generate
    let { volume, devices, oldDevice, devnames, oldMode } = await this.verifyDevices(devs)

    // mount need repair volume as degraded mode
    await this.mountRVolume(devices.map(d => d.name), oldDevice.path, volume.mountpoint)

    // do repair
    await this.doRepairAsync({ oldMode, devices, mountpoint: volume.mountpoint, devnames, oldDevice }, mode)

    return await this.saveBoundVolumeAsync(volume.uuid)
  }

  /**
   * step 1 , verify devices
   * @param {array} devices
   * {
   *  volume, // need repair volume
   *  devices, // devices
   *  oldDevice, // device in volume which can work again, assgin (boundVolume and volume)
   *  devnames, // devices items's devnames
   * }
   */
  async verifyDevices (devices) {
    let storage, volume, volumeDevice, boundVolume, oldDevice, devnames = []

    storage = await probeAsync(this.ctx.conf.storage)

    boundVolume = this.ctx.volumeStore.data
    if (!boundVolume) throw new Error('have not bound volume')
    if (boundVolume.devices.length !== 2) throw new Error('boundVolume only 1 device')
    let volumeUUID = boundVolume.uuid

    volume = storage.volumes.find(v => v.uuid === volumeUUID)
    if (!volume) throw new Error('boundVolume not found')
    if (!volume.missing) throw new Error('volume is complete')
    volumeDevice = volume.devices.filter(d => !!d.name)
    if (volumeDevice.length !== 1) throw new Error('volume can not repair, no block found')
    // vaildate
    devices.forEach(d => {
      if (isNonEmptyString(d.name)) {
        let block = storage.blocks.find(b => b.name === d.name)
        if (!block) throw new Error(`device ${ d.name } not found`)
        if (isNonEmptyString(d.model)) {
          if (block.model !== d.model) throw new Error(d.name + ' model mismatch')
          if (block.serial !== d.serial) throw new Error(d.name + ' serial mismatch')
        }
        d.model = block.model
        d.serial = block.serial
      } else {
        let block = storage.blocks.find(b => b.model === d.model && b.serial === d.serial)
        if (!block) throw new Error('device not found')
        d.name = block.name
      }
    })

    let vd = devices.find(d => d.name === volumeDevice[0].name)
    oldDevice = boundVolume.devices.find(d => d.model === vd.model && d.serial === vd.serial)
    if (!oldDevice) throw new Error('old device not found')
    oldDevice = Object.assign({}, oldDevice, volumeDevice[0])

    console.log('=====================')
    console.log('OldDevice: ', oldDevice)

    console.log('Devices: ', devices)
    console.log('=====================')

    if (!devices.find(d => d.name === oldDevice.name))
      throw new Error('devices not contain any old device')

    for (let i = 0; i < devices.length; i++) {
      let block = storage.blocks.find(blk => blk.name === devices[i].name)
      if (!block) throw new Error(`device ${devices[i]} not found`)
      if (!block.isDisk) throw new Error(`device ${devices[i]} is not a disk`)
      if (block.unformattable) throw new Error(`device ${devices[i]} is not formattable`)
      devnames.push(Object.assign(devices[i], { devname:block.devname }))
    }

    let oldMode = boundVolume.usage.data.mode.toLowerCase()

    return { volume, devices, oldDevice, devnames, oldMode }
  }

  async mountRVolume (deviceNames, path, mountpoint) {
    let storage = await probeAsync(this.ctx.conf.storage)
    await umountBlocksAsync(storage, deviceNames)
    // mount as degraded
    try {
      await child.execAsync(`mount -t btrfs -o degraded ${ path } ${ mountpoint }`)
    } catch(e) {
      Promise.delay(100)
      await child.execAsync(`mount -t btrfs -o degraded ${ path } ${ mountpoint }`)
    }
    await child.execAsync('partprobe')
  }

  async doRepairAsync ({ oldMode, devices, mountpoint, devnames, oldDevice }, mode) {
    if (oldMode === 'single') {
      if (devices.length == 1) {
        if (mode !== 'single') throw new Error('Only can make single in one device')
        await child.execAsync(`btrfs balance start -f -mconvert=single ${ mountpoint }`)
        await child.execAsync(`btrfs device delete missing ${ mountpoint }`)
        await child.execAsync(`btrfs balance start -f -mconvert=dup ${ mountpoint }`)
      } else {
        let addDevice = devnames.filter(d => d.name !== oldDevice.name ).map(v => v.devname)
        await child.execAsync(`btrfs device add -f ${ addDevice.join(' ') } ${ mountpoint }`)
        await child.execAsync(`btrfs device delete missing ${ mountpoint }`)
        if (!mode === 'single') { // raid1
          await child.execAsync(`btrfs balance start -f -dconvert=raid1 ${ mountpoint }`)
        }
      }
    } else if (oldMode === 'raid1') {
      if (devices.length == 1) {
        if (mode !== 'single') throw new Error('Only can make single in one device')
        await child.execAsync(`btrfs balance start -f -mconvert=single -dconvert=single ${ mountpoint }`)
        await child.execAsync(`btrfs device delete missing ${ mountpoint }`)
        await child.execAsync(`btrfs balance start -f -mconvert=dup ${ mountpoint }`)
      } else {
        let addDevice = devnames.filter(d => d.name !== oldDevice.name ).map(v => v.devname)
        await child.execAsync(`btrfs device add -f ${ addDevice.join(' ') } ${ mountpoint }`)
        await child.execAsync(`btrfs device delete missing ${ mountpoint }`)
        if (mode === 'single') { // raid1
          await child.execAsync(`btrfs balance start -f -dconvert=single  ${ mountpoint }`)
        }
      }
    } else {
      throw new Error('unsupport old mode')
    }
  }

  async saveBoundVolumeAsync (volumeUUID) {
    let storage = await probeAsync(this.ctx.conf.storage)
    let newVolume = storage.volumes.find(v => v.uuid === volumeUUID)
    if (!newVolume) throw new Error('cannot find a volume containing expected block name')

    // ensure bound volume data format
    if (!newVolume.usage || !newVolume.usage.system || !newVolume.usage.metadata || !newVolume.usage.data) {
      console.log(newVolume)
      throw new Error('volume usage not properly detected')
    }
    // update boundVolume
    let newBoundVolume = this.createBoundVolume(storage, newVolume, this.ctx.volumeStore.data.id)
    console.log('=======newBoundVolume======')
    console.log(newBoundVolume)
    console.log('============================')
    return new Promise((resolve, reject) => {
      this.ctx.volumeStore.save(newBoundVolume, err => {
        if (err) {
          reject(err)
        } else {
          resolve(newBoundVolume)
        }
      })
    })
  }
}

/**

*/
class Boot extends EventEmitter {
  /**
  Creates a Boot object

  @param {object} opts - options
  @param {Configuration} opts.configuration - application-wide configuration
  @param {object} opts.fruitmixOpts - fruitmix options
  */
  constructor (opts) {
    super()

    if (!opts.configuration) throw new Error(`boot requires a configuration`)

    this.conf = opts.configuration
    this.fruitmixOpts = opts.fruitmixOpts
    this.error = null
    this.fruitmix = null

    this.preset = undefined

    this._storage = undefined
    Object.defineProperty(this, 'storage', {
      get () {
        return this._storage
      },
      set (value) {
        let oldValue = this._storage
        this._storage = value
        if (this.fruitmix) this.fruitmix.setStorage(value)
        process.nextTick(() => this.emit('StorageUpdate', value, oldValue))
      }
    })

    if (!IS_WINAS) {
      this.prepareChassisDirs(err => {
        if (err) {
          // will halt boot @ pending state after probing
          this.error = err
        } else {
          this.volumeStore = new DataStore(this.storeOpts('volume'))
          this.volumeStore.on('Update', () => this.state.boundVolumeUpdated())
  
          // for preset, preserve a copy
          this.presetStore = new DataStore(this.storeOpts('preset'))
          this.presetStore.once('Update', data => {
            if (data) {
              this.preset = { state: 'PENDING', data }
              this.presetStore.save(null, () => {})
            } else {
              this.preset = null
            }
            this.state.presetLoaded()
          })
        }
      })
    } 

    new Probing(this)

    this.storageUpdater = new StorageUpdater(this.conf)
    this.storageUpdater.on('update', this.storageUpdate.bind(this))
  }

  storageUpdate (data) {
    this.storage = data
  }

  stateName () {
    return this.state.constructor.name
  }

  storeOpts (name) {
    return {
      file: path.join(this.conf.chassis.dir, `${name}.json`),
      tmpDir: path.join(this.conf.chassis.tmpDir, name)
    }
  }

  async prepareChassisDirsAsync () {
    let { dir, tmpDir } = this.conf.chassis
    await mkdirpAsync(dir)
    await rimrafAsync(tmpDir)
    mkdirpAsync(tmpDir)
  }

  prepareChassisDirs (callback) {
    this.prepareChassisDirsAsync()
      .then(() => callback())
      .catch(e => (console.log(e), callback(e)))
  }

  bootable () {
    if (!this.boundUser) return false // no bound user
    if (!this.volumeStore.data) return false // no bound volume

    let vol = this.storage.volumes.find(v => v.uuid === this.volumeStore.data.uuid)
    if (!vol) return false // bound volume not found
    if (vol.missing) return false // bound volume has missing device
    if (!Array.isArray(vol.users)) return false // users.json not ready

    let firstUser = vol.users.find(u => u.isFirstUser === true)
    if (!firstUser) return false // firstUser not found
    if (firstUser.winasUserId !== this.boundUser.id) return false
    return true
  }

  setBoundUser (user) {
    this.boundUser = user
    if (this.state.constructor.name !== 'Started') return
    this.fruitmix.bindFirstUser(user)
  }

  view () {
    return {
      state: this.state.constructor.name.toUpperCase(),
      error: this.state.error ? {
        message: this.state.error.message,
        code: this.state.error.code
      } : undefined,
      boundUser: this.boundUser ? {
        winasUserId: this.boundUser.id
      } : this.boundUser,
      boundVolume: this.volumeStore && this.volumeStore.data,
      storage: this.storage,
      preset: this.preset
    }
  }

  init (target, mode, callback) {
    return process.nextTick(() => callback(new Error('Error Operation')))
    // this.state.init(target, mode, callback)
  }

  import (volumeUUID, callback) {
    return process.nextTick(() => callback(new Error('Error Operation')))
    //this.state.import(volumeUUID, callback)
  }

  repair (devices, mode, callback) {
    return process.nextTick(() => callback(new Error('Error Operation')))
    //this.state.repair(devices, mode, callback)
  }

  add (devices, mode, callback) {
    return process.nextTick(() => callback(new Error('Error Operation')))
    //this.state.add(devices, mode, callback)
  }

  remove (devices, callback) {
    return process.nextTick(() => callback(new Error('Error Operation')))
    //this.state.remove(devices, callback)
  }

  
  uninstall (user, props,callback) {
    return process.nextTick(() => callback(new Error('Error Operation')))
  }

  format (target, callback) {
    this.state.format(target, callback)
  }

  // TODO: wait definition
  resetToFactory(user, autoReboot, callback) {
    return process.nextTick(() => callback(new Error('Error Operation')))
  }

  async ejectUSBAsync (target) {
    if (!this.storage) throw new Error('no storage')
    let block = this.storage.blocks.find(b => b.name === target)
    if (!block) throw new Error('block not found')
    if (!block.isUSB) throw new Error('block not usb device')
    if (block.isPartitioned) { // parent block
      let subBlocks = this.storage.blocks.filter(b => b.parentName === target)
      if (!subBlocks.length) throw new Error('block is partitioned, but subBlock not found')
      let mountedSB = subBlocks.filter(s => s.isMounted)
      for (let i = 0; i < mountedSB.length; i++) {
        await child.execAsync(`udisksctl unmount -b ${ mountedSB[i].devname }`)
      }
      await child.execAsync(`udisksctl power-off -b ${ subBlocks[0].devname }`)
      
    } else if (block.isPartition) {//sub block
      if (!isNonEmptyString(block.parentName)) throw new Error('block is partition, but parentName not found')
      let subBlocks = this.storage.blocks.filter(b => b.parentName === block.parentName)
      let mountedSB = subBlocks.filter(s => s.isMounted)
      for (let i = 0; i < mountedSB.length; i++) {
        await child.execAsync(`udisksctl unmount -b ${ mountedSB[i].devname }`)
      }
      await child.execAsync(`udisksctl power-off -b ${ block.devname }`)

    } else {
      if (block.isMounted) {
        await child.execAsync(`udisksctl unmount -b ${ block.devname }`)
      }
      await child.execAsync(`udisksctl power-off -b ${ block.devname }`)
    }
  }

  ejectUSB(target, callback) {
    this.ejectUSBAsync(target)
      .then(() => callback(null))
      .catch(e => callback(e))
  }

  getStorage () {
  }

  GET (user, props, callback) {
    process.nextTick(() => callback(null, this.view()))
  }

  PATCH (user, props, callback) {
    let target = props.target
    let mode = props.mode
    this.init(target, mode, callback)
  }

  POST (user, props, callback) {
    let target = props.target
    this.format(target, callback)
  }

  PATCH_BOOT (user, props, callback) {
    if (props.hasOwnProperty('state')) {
      if (props.state !== 'poweroff' && props.state !== 'reboot')
        return callback(Object.assign(new Error('invalid state'), { status: 400 }))
      setTimeout(() => child.exec(props.state), 2000)
      callback(null)
    } else return callback(Object.assign(new Error('invalid props'), { status: 400 }))
  }

  findEmbedVolume() {
    if (!this.storage) return
    let block = this.storage.blocks.find(v => v.fileSystemUUID !== '0cbc36fa-3b85-40af-946e-f15dce29d86b' && v.isUSB && v.isBtrfs)
    if (!block) return
    return this.storage.volumes.find(v => v.fileSystemUUID === block.fileSystemUUID && v.isMounted && !v.isMissing)
  }

  GET_BoundVolume (user, callback) {
    let vol = this.findEmbedVolume()
    if (!vol) return callback(new Error('embed volume not found'))
    
    child.exec(`df -P "${vol.mountpoint}"`, (err, stdout) => {
      if (!err) {
        let lines = stdout.toString().trim().split('\n')
        if (lines.length === 2) {
          let xs = lines[1].split(' ').filter(x => !!x)
          if (xs.length === 6) {
            let usage = {
              total: parseInt(xs[1]),
              used: parseInt(xs[2]),
              available: parseInt(xs[3])
            }
            let total
            let sizeArr = vol.devices.map(d => d.size).sort((a, b) => a > b ? 1 : a < b ? -1 : 0)
            if (vol.usage && vol.usage.data && vol.usage.data.mode.toLowerCase() === 'raid1') {
              let max = sizeArr.pop()
              let offmax = sizeArr.reduce((acc, a) => a + acc, 0)
              total = max > offmax ? offmax : (offmax + max)/2
            } else {
              total = sizeArr.reduce((acc, a) => a + acc, 0)
            }
            usage.total = total / 1024
            callback(null, usage)
          }
        }
      } else {
        callback(err)
      }
    })
  }
}

module.exports = Boot