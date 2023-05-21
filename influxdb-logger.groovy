/* groovylint-disable LineLength, MethodParameterTypeRequired, MethodReturnTypeRequired, MethodSize, NestedBlockDepth, NoDef, ParameterReassignment, PublicMethodsBeforeNonPublicMethods, UnnecessaryGString, UnnecessaryGetter, UnnecessaryObjectReferences, UnusedMethodParameter, VariableTypeRequired */
/*****************************************************************************************************************
 *  Source: https://github.com/HubitatCommunity/InfluxDB-Logger
 *
 *  Raw Source: https://raw.githubusercontent.com/HubitatCommunity/InfluxDB-Logger/master/influxdb-logger.groovy
 *
 *  Forked from: https://github.com/codersaur/SmartThings/tree/master/smartapps/influxdb-logger
 *  Original Author: David Lomas (codersaur)
 *  Previous Author: Joshua Marker (tooluser)
 *  Hubitat Elevation version maintained by HubitatCommunity (https://github.com/HubitatCommunity/InfluxDB-Logger)
 *
 *  License:
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License. You may obtain a copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 *   for the specific language governing permissions and limitations under the License.
 *
 *   Modifcation History
 *   Date       Name            Change
 *   2019-02-02 Dan Ogorchock   Use asynchttpPost() instead of httpPost() call
 *   2019-09-09 Caleb Morse     Support deferring writes and doing bulk writes to influxdb
 *   2022-06-20 Denny Page      Remove nested sections for device selection.
 *   2023-01-08 Denny Page      Address whitespace related lint issues. No functional changes.
 *   2023-01-09 Craig King      Added InfluxDb2.x support.
 *   2023-01-12 Denny Page      Automatic migration of Influx 1.x settings.
 *   2023-01-15 Denny Page      Clean up various things:
 *                              Remove Group ID/Name which are not supported on Hubitat.
 *                              Remove Location ID and Hub ID which are not supported on Hubitat (always 1).
 *                              Remove blocks of commented out code.
 *                              Don't set page sections hidden to false where hideable is false.
 *                              Remove state.queuedData.
 *   2023-01-22 PJ              Add filterEvents option for subscribe.
 *                              Fix event timestamps.
 *   2023-01-23 Denny Page      Allow multiple instances of the application to be installed.
 *                              NB: This requires Hubitat 2.2.9 or above.
 *   2023-01-25 Craig King      Updated Button selection to valid capability for Hubitat
 *   2023-02-16 PJ              Add error message to log for http response >= 400
 *                              Allow ssl cert verification to be disabled (self signed certificates)
 *   2023-02-26 Denny Page      Cleanup and rationalize UI
 *                              Use time since first data value to trigger post rather than periodic timer
 *                              Only create a keep alive event (softpoll) when no real event has been seen
 *                              Cleanup and rationalize logging
 *                              Further code cleanup
 *   2023-02-28 Denny Page      Retry failed posts
 *                              Enhance post logging
 *                              Allow Hub Name and Location tags to be disabled for device events
 *                              Further code cleanup
 *   2023-03-04 Denny Page      Clean up event processing code
 *                              Fix button event handling
 *                              Fix thermostat fan mode event handling
 *                              Fix threeAxis event encoding
 *                              Add device event handling for filters, gas detectors, power source
 *                              Remove handling for non-existent device capabilities
 *                              Move unnecessary info messages to debug
 *                              Disable debug logging of post data which drives hubs into the ground
 *                              Provide info logging of event data to replace post data logging
 *                              Allow backlog to be set as low as 1, allowing bad records to be cleared
 *   2023-03-12 Denny Page      Use a unified device type / attribute map (deviceTypeMap)
 *                              Unify advanced and non-advanced device selection processing
 *                              Move device event encoding out to a separate function
 *                              Enhance queueToInfluxDb to accept a list of events
 *                              Complete rewrite of softpoll (take advantage of queueToInfluxDb lists)
 *                              Remove unnecessary state variables
 *                              Don't re-schedule batch post based on batch size, wait for existing timer
 *                              Improve backlog warnings
 *                              Lower backlog limits to prevent issues with app database size
 *                              Normalize Hub information logging
 *   2023-03-14 Denny Page      If post of size one fails, log the actual failed record
 *                              Ignore momentary attributes for keep alive
 *                              Display full uri in config page for convenience
 *   2023-03-15 Denny Page      Always treat valid numbers (such as buttons) as numeric values for InfluxDB
 *   2023-03-16 Denny Page      Fix issue of quotes surrounding hub name
 *                              Fix issue of invalid three axis values being posted as a string
 *   2023-03-18 Denny Page      Fix valve attribute
 *                              Clean up logging
 *   2023-04-24 Denny Page      Don't send null units to InfluxDB
 *****************************************************************************************************************/

// Note: Items marked as "Migration" are intended to be kept for a period of time and then be removed circa end of 2023

definition(
    name: "InfluxDB Logger",
    namespace: "nowhereville",
    author: "Hubitat Community",
    description: "Log device states to InfluxDB",
    category: "Utility",
    importUrl: "https://raw.githubusercontent.com/HubitatCommunity/InfluxDB-Logger/master/influxdb-logger.groovy",
    iconUrl: "",
    iconX2Url: "",
    iconX3Url: "",
    singleThreaded: true
)

import groovy.transform.Field

// Device type list
@Field static final Map<String,Map> deviceTypeMap = [
    'accelerometers': [ title: 'Accelerometers', capability: 'accelerationSensor', attributes: ['acceleration'] ],
    'alarms': [ title: 'Alarms', capability: 'alarm', attributes: ['alarm'] ],
    'batteries': [ title: 'Batteries', capability: 'battery', attributes: ['battery'] ],
    'beacons': [ title: 'Beacons', capability: 'beacon', attributes: ['presence'] ],
    'buttons': [ title: 'Buttons', capability: 'pushableButton', attributes: ['pushed', 'doubleTapped', 'held', 'released'] ],
    'cos': [ title: 'Carbon Monoxide Detectors', capability: 'carbonMonoxideDetector', attributes: ['carbonMonoxide'] ],
    'co2s': [ title: 'Carbon Dioxide Detectors', capability: 'carbonDioxideMeasurement', attributes: ['carbonDioxide'] ],
    'colors': [ title: 'Color Controllers', capability: 'colorControl', attributes: ['hue', 'saturation', 'color'] ],
    'consumables': [ title: 'Consumables', capability: 'consumable', attributes: ['consumableStatus'] ],
    'contacts': [ title: 'Contact Sensors', capability: 'contactSensor', attributes: ['contact'] ],
    'currentMeters': [ title: 'Current Meters', capability: 'currentMeter', attributes: ['amperage'] ],
    'doorsControllers': [ title: 'Door Controllers', capability: 'doorControl', attributes: ['door'] ],
    'energyMeters': [ title: 'Energy Meters', capability: 'energyMeter', attributes: ['energy'] ],
    'filters': [ title: 'Filters', capability: 'filterStatus', attributes: ['filterStatus'] ],
    'gasDetectors': [ title: 'Gas Detectors', capability: 'gasDetector', attributes: ['naturalGas'] ],
    'humidities': [ title: 'Humidity Meters', capability: 'relativeHumidityMeasurement', attributes: ['humidity'] ],
    'illuminances': [ title: 'Illuminance Meters', capability: 'illuminanceMeasurement', attributes: ['illuminance'] ],
    'liquidFlowMeters': [ title: 'Liquid Flow Meters', capability: 'liquidFlowRate', attributes: ['rate'] ],
    'locks': [ title: 'Locks', capability: 'lock', attributes: ['lock'] ],
    'motions': [ title: 'Motion Sensors', capability: 'motionSensor', attributes: ['motion'] ],
    'musicPlayers': [ title: 'Music Players', capability: 'musicPlayer', attributes: ['status', 'level', 'trackDescription', 'trackData', 'mute'] ],
    'peds': [ title: 'Pedometers', capability: 'stepSensor', attributes: ['steps', 'goal'] ],
    'phMeters': [ title: 'pH Meters', capability: 'pHMeasurement', attributes: ['pH'] ],
    'powerMeters': [ title: 'Power Meters', capability: 'powerMeter', attributes: ['power'] ],
    'powerSources': [ title: 'Power Sources', capability: 'powerSources', attributes: ['powerSource'] ],
    'presences': [ title: 'Presence Sensors', capability: 'presenceSensor', attributes: ['presence'] ],
    'pressures': [ title: 'Pressure Sensors', capability: 'pressureMeasurement', attributes: ['pressure'] ],
    'shockSensors': [ title: 'Shock Sensors', capability: 'shockSensor', attributes: ['shock'] ],
    'signalStrengthMeters': [ title: 'Signal Strength Meters', capability: 'signalStrength', attributes: ['lqi', 'rssi'] ],
    'sleepSensors': [ title: 'Sleep Sensors', capability: 'sleepSensor', attributes: ['sleeping'] ],
    'smokeDetectors': [ title: 'Smoke Detectors', capability: 'smokeDetector', attributes: ['smoke'] ],
    'soundSensors': [ title: 'Sound Sensors', capability: 'soundSensor', attributes: ['sound'] ],
    'spls': [ title: 'Sound Pressure Level Sensors', capability: 'soundPressureLevel', attributes: ['soundPressureLevel'] ],
    'switches': [ title: 'Switches', capability: 'switch', attributes: ['switch'] ],
    'switchLevels': [ title: 'Switch Levels', capability: 'switchLevel', attributes: ['level'] ],
    'tamperAlerts': [ title: 'Tamper Alerts', capability: 'tamperAlert', attributes: ['tamper'] ],
    'temperatures': [ title: 'Temperature Sensors', capability: 'temperatureMeasurement', attributes: ['temperature'] ],
    'thermostats': [ title: 'Thermostats', capability: 'thermostat', attributes: ['temperature', 'heatingSetpoint', 'coolingSetpoint', 'thermostatSetpoint', 'thermostatMode', 'thermostatFanMode', 'thermostatOperatingState', 'thermostatSetpointMode', 'scheduledSetpoint'] ],
    'threeAxis': [ title: 'Three-axis (Orientation) Sensors', capability: 'threeAxis', attributes: ['threeAxis'] ],
    'touchs': [ title: 'Touch Sensors', capability: 'touchSensor', attributes: ['touch'] ],
    'uvs': [ title: 'UV Sensors', capability: 'ultravioletIndex', attributes: ['ultravioletIndex'] ],
    'valves': [ title: 'Valves', capability: 'valve', attributes: ['valve'] ],
    'volts': [ title: 'Voltage Meters', capability: 'voltageMeasurement', attributes: ['voltage'] ],
    'waterSensors': [ title: 'Water Sensors', capability: 'waterSensor', attributes: ['water'] ],
    'windowShades': [ title: 'Window Shades', capability: 'windowShade', attributes: ['windowShade'] ]
]

// Momentary attributes that should be ignored for keep alive
@Field static final List<String> momentaryAttributes = [
    'pushed', 'doubleTapped', 'held', 'released'
]

@Field static final Map<Integer,String> logOptions = [
    0 : "None",
    1 : "Error",
    2 : "Warning",
    3 : "Info",
    4 : "Debug"
]
@Field static final Integer logNone  = 0
@Field static final Integer logError = 1
@Field static final Integer logWarn  = 2
@Field static final Integer logInfo  = 3
@Field static final Integer logDebug = 4

preferences {
    page(name: "setupMain")
    page(name: "connectionPage")
}

def setupMain() {
    dynamicPage(name: "setupMain", title: "<h2>InfluxDB Logger</h2>", install: true, uninstall: true) {
        section("<h3>\nGeneral Settings:</h3>") {
            input "appName", "text", title: "Aplication Name", multiple: false, required: true, submitOnChange: true, defaultValue: app.getLabel()

            input(
                name: "configLoggingLevelIDE",
                title: "System log level - messages with this level and higher will be sent to the system log",
                type: "enum",
                options: logOptions,
                defaultValue: "${logWarn}",
                required: false
            )
        }

        section("\n<h3>InfluxDB Settings:</h3>") {
            href(
                name: "href",
                title: "InfluxDB connection",
                description : prefDatabaseHost == null ? "Configure database connection parameters" : uriString(),
                required: true,
                page: "connectionPage"
            )
            input(
                name: "prefBatchTimeLimit",
                title: "Batch time limit - maximum number of seconds before writing a batch to InfluxDB (range 1-300)",
                type: "number",
                range: "1..300",
                defaultValue: "60",
                required: true
            )
            input(
                name: "prefBatchSizeLimit",
                title: "Batch size limit - maximum number of events in a batch to InfluxDB (range 1-250)",
                type: "number",
                range: "1..250",
                defaultValue: "50",
                required: true
            )
            input(
                name: "prefBacklogLimit",
                title: "Backlog size limit - maximum number of queued events before dropping failed posts (range 1-10000)",
                type: "number",
                range: "1..10000",
                defaultValue: "5000",
                required: true
            )
        }

        section("\n<h3>Event Handling:</h3>") {
            input(
                // NB: Called prefSoftPollingInterval for backward compatibility with prior versions
                name: "prefSoftPollingInterval",
                title: "Post keep alive events (aka softpoll) - check every softpoll interval and re-post last value if a new event has not occurred in this time",
                type: "enum",
                options: [
                    "0" : "disabled",
                    "1" : "1 minute (not recommended)",
                    "5" : "5 minutes",
                    "10" : "10 minutes",
                    "15" : "15 minutes",
                    "30" : "30 minutes",
                    "60" : "60 minutes",
                    "180" : "3 hours"
                ],
                defaultValue: "15",
                submitOnChange: true,
                required: true
            )
            if (prefSoftPollingInterval?.toInteger()) {
                input "prefPostHubInfo", "bool", title:"Post Hub information (IP, firmware, uptime, mode, sunrise/sunset) to InfluxDB", defaultValue: false
            }
            input "includeHubInfo", "bool", title:"Include Hub Name as a tag for device events", defaultValue: true
            input "filterEvents", "bool", title:"Only post device events to InfluxDB when the data value changes", defaultValue: true
        }

        section("Devices To Monitor:", hideable:true, hidden:false) {
            input "accessAllAttributes", "bool", title:"Advanced attribute seletion?", defaultValue: false, submitOnChange: true

            if (accessAllAttributes) {
                input name: "allDevices", type: "capability.*", title: "Selected Devices", multiple: true, required: false, submitOnChange: true

                settings.allDevices.each { device ->
                    deviceId = device.getId()
                    attrList = device.getSupportedAttributes().unique()
                    if (attrList) {
                        options = []
                        attrList.each { attr ->
                            options.add("${attr}")
                        }
                        input name:"attrForDev${deviceId}", type: "enum", title: "$device", options: options.sort(), multiple: true, required: false, submitOnChange: true
                    }
                }
            }
            else {
                deviceTypeMap.each { name, entry ->
                    input "${name}", "capability.${entry.capability}", title: "${entry.title}", multiple: true, required: false
                }
            }
        }
        
        section("Variables To Monitor:", hideable:true, hidden:false) {    
            input "booleanVariables", "enum", title: "Select Boolean Variables", multiple: true, submitOnChange: true,
                options: getAllGlobalVars().findAll{
                    it.value.type == "boolean"
                }.keySet().collect().sort{it.capitalize()}

            input "numberVariables", "enum", title: "Select Number Variables", multiple: true, submitOnChange: true,
                options: getAllGlobalVars().findAll{
                    it.value.type == "integer"
                }.keySet().collect().sort{it.capitalize()}

            input "decimalVariables", "enum", title: "Select Decimal Variables", multiple: true, submitOnChange: true,
                options: getAllGlobalVars().findAll{
                    it.value.type == "bigdecimal"
                }.keySet().collect().sort{it.capitalize()}

            input "stringVariables", "enum", title: "Select String Variables", multiple: true, submitOnChange: true,
                options: getAllGlobalVars().findAll{
                    it.value.type == "string"
                }.keySet().collect().sort{it.capitalize()}
            
            input "datetimeVariables", "enum", title: "Select DateTime Variables", multiple: true, submitOnChange: true,
                options: getAllGlobalVars().findAll{
                    it.value.type == "datetime"
                }.keySet().collect().sort{it.capitalize()}
        }
    }
}

def connectionPage() {
    dynamicPage(name: "connectionPage", title: "Connection Properties", install: false, uninstall: false) {
        section {
            input "prefDatabaseTls", "bool", title:"Use TLS?", defaultValue: false, submitOnChange: true
            if (prefDatabaseTls) {
                input "prefIgnoreSSLIssues", "bool", title:"Ignore SSL cert verification issues", defaultValue:false
            }

            input "prefDatabaseHost", "text", title: "Host", defaultValue: "", required: true
            input "prefDatabasePort", "text", title : "Port", defaultValue : prefDatabaseTls ? "443" : "8086", required : false
            input(
                name: "prefInfluxVer",
                title: "Influx Version",
                type: "enum",
                options: [
                    "1" : "v1.x",
                    "2" : "v2.x"
                ],
                defaultValue: "1",
                submitOnChange: true
            )
            if (prefInfluxVer == "1") {
                input "prefDatabaseName", "text", title: "Database Name", defaultValue: "Hubitat", required: true
            }
            else if (prefInfluxVer == "2") {
                input "prefOrg", "text", title: "Org", defaultValue: "", required: true
                input "prefBucket", "text", title: "Bucket", defaultValue: "", required: true
            }
            input(
                name: "prefAuthType",
                title: "Authorization Type",
                type: "enum",
                options: [
                    "none" : "None",
                    "basic" : "Username / Password",
                    "token" : "Token"
                ],
                defaultValue: "none",
                submitOnChange: true
            )
            if (prefAuthType == "basic") {
                input "prefDatabaseUser", "text", title: "Username", defaultValue: "", required: true
                input "prefDatabasePass", "text", title: "Password", defaultValue: "", required: true
            }
            else if (prefAuthType == "token") {
                input "prefDatabaseToken", "text", title: "Token", required: true
            }
        }
    }
}

/**
 *  installed()
 *
 *  Runs when the app is first installed.
 **/
void installed() {
    state.installedAt = now()
    state.loggerQueue = []
    updated()
    log.info "${app.label}: Installed"
}

/**
 *  uninstalled()
 *
 *  Runs when the app is uninstalled.
 **/
void uninstalled() {
    log.info "${app.label}: Uninstalled"
}

/**
 *  updated()
 *
 *  Runs when app settings are changed.
 **/
void updated() {
    // Update application name
    app.updateLabel(settings.appName)
    logger("${app.label}: Updated", logInfo)

    // Database config:
    setupDB()

    // Clear out any prior subscriptions
    unsubscribe()

    // Create device subscriptions
    Map<String,List> deviceAttrMap = getDeviceAttrMap()
    deviceAttrMap.each { device, attrList ->
        attrList.each { attr ->
            logger("Subscribing to ${device}: ${attr}", logInfo)
            subscribe(device, attr, handleEvent, ["filterEvents": filterEvents])
        }
    }

    // Subscribe to system start
    subscribe(location, "systemStart", hubRestartHandler)

    // Subscribe to mode events if requested
    if (prefPostHubInfo) {
        subscribe(location, "mode", handleModeEvent)
    }
    
    // Subscribe to variables
    (booleanVariables+numberVariables+decimalVariables+stringVariables+datetimeVariables).each { name ->
        subscribe(location, "variable:" + name, handleVariableEvent)
        logger("Subscribing to variable ${name}", logInfo) // TODO: change back to Info
    }

    // Clear out any prior schedules
    unschedule()

    // Set up softpoll if requested
    // NB: This is called softPoll to maintain backward compatibility wirh prior versions
    state.softPollingInterval = settings.prefSoftPollingInterval.toInteger()
    switch (state.softPollingInterval) {
        case 1:
            runEvery1Minute(softPoll)
            break
        case 5:
            runEvery5Minutes(softPoll)
            break
        case 10:
            runEvery10Minutes(softPoll)
            break
        case 15:
            runEvery15Minutes(softPoll)
            break
        case 30:
            runEvery30Minutes(softPoll)
            break
        case 60:
            runEvery1Hour(softPoll)
            break
        case 180:
            runEvery3Hours(softPoll)
            break
    }

    // Flush any pending batch
    runIn(1, writeQueuedDataToInfluxDb)

    // Migration: Clean up old state variables
    state.remove("deviceAttributes")
    state.remove("deviceList")
    state.remove("loggingLevelIDE")
    state.remove("options")
    state.remove("postExpire")
    state.remove("queuedData")
    state.remove("selectedAttr")
    state.remove("writeInterval")
    app.removeSetting("prefLogHubProperties")
    app.removeSetting("prefLogLocationProperties")
    app.removeSetting("prefLogModeEvents")
}

/**
 *  getDeviceAttrMap()
 *
 *  Build a device attribute map.
 *
 * If using attribute selection, a device will appear only once in the array, with one or more attributes.
 * If using capability selection, a device may appear multiple times in the array, each time with a single attribue.
 **/
private Map<String,List> getDeviceAttrMap() {
    deviceAttrMap = [:]

    if (settings.accessAllAttributes) {
        settings.allDevices.each { device ->
            deviceId = device.getId()
            deviceAttrMap[device] = settings["attrForDev${deviceId}"]
        }
    }
    else {
        deviceTypeMap.each { name, entry ->
            deviceList = settings."${name}"
            if (deviceList) {
                deviceList.each { device ->
                    deviceAttrMap[device] = entry.attributes
                }
            }
        }
    }

    return deviceAttrMap
}

/**
 * hubRestartHandler()
 *
 * Handle hub restarts.
**/
void hubRestartHandler(evt) {
    if (prefPostHubInfo) {
        handleModeEvent(null)
    }

    if (state.loggerQueue?.size()) {
        runIn(60, writeQueuedDataToInfluxDb)
    }
}

/**
 *  encodeDeviceEvent(evt)
 *
 *  Builds data to send to InfluxDB.
 *   - Escapes and quotes string values.
 *   - Calculates logical binary values where string values can be
 *     represented as binary values (e.g. contact: closed = 1, open = 0)
 **/
private String encodeDeviceEvent(evt) {
    //
    // This switch handles special processing for various events types
    //
    // Entries in the switch may optionally set the unit, value, or valueBinary fields.
    // Any fields set in the switch are "final" and do not receive further processing,
    // so all field assignments must be escaped as necessary.
    //
    // If not set in the switch, the unit and value fields will be defaulted as follows:
    //
    //   Unit:   If valueBinary has been set in the switch, the unit tag will be the
    //           will be set to the name of the event (evt.name). Otherwise, the unit
    //           tag will be set to the unit of the event (evt.unit). Both will be
    //           escaped.
    //   Value:  If the value in the event (evt.value) is a valid number, the value
    //           will be InfluxDB's default numeric type (float). Othersie, the value
    //           will be escaped and inclosed in double quotes as a string.
    //
    String unit = ''
    String value = ''
    String valueBinary = ''
    switch (evt.name) {
        case 'acceleration':
            // binary value: active = 1, <any other value> = 0
            valueBinary = (evt.value == 'active') ? '1i' : '0i'
            break
        case 'alarm':
            // binary value: <any other value> = 1, off = 0
            valueBinary = (evt.value == 'off') ? '0i' : '1i'
            break
        case 'carbonMonoxide':
            // binary value: detected = 1, <any other value> = 0
            valueBinary = (evt.value == 'detected') ? '1i' : '0i'
            break
        case 'consumableStatus':
            // binary value: good = 1, <any other value> = 0
            valueBinary = (evt.value == 'good') ? '1i' : '0i'
            break
        case 'contact':
            // binary value: closed = 1, <any other value> = 0
            valueBinary = (evt.value == 'closed') ? '1i' : '0i'
            break
        case 'door':
            // binary value: closed = 1, <any other value> = 0
            valueBinary = (evt.value == 'closed') ? '1i' : '0i'
            break
        case 'filterStatus':
            // binary value: normal = 1, <any other value> = 0
            valueBinary = (evt.value == 'normal') ? '1i' : '0i'
            break
        case 'lock':
            // binary value: locked = 1, <any other value> = 0
            valueBinary = (evt.value == 'locked') ? '1i' : '0i'
            break
        case 'motion':
            // binary value: active = 1, <any other value> = 0
            valueBinary = (evt.value == 'active') ? '1i' : '0i'
            break
        case 'mute':
            // binary value: muted = 1, <any other value> = 0
            valueBinary = (evt.value == 'muted') ? '1i' : '0i'
            break
        case 'naturalGas':
            // binary value: detected = 1, <any other value> = 0
            valueBinary = (evt.value == 'detected') ? '1i' : '0i'
            break
        case 'powerSource':
            // binary value: mains = 1, <any other value> = 0
            valueBinary = (evt.value == 'mains') ? '1i' : '0i'
            break
        case 'presence':
            // binary value: present = 1, <any other value> = 0
            valueBinary = (evt.value == 'present') ? '1i' : '0i'
            break
        case 'shock':
            // binary value: detected = 1, <any other value> = 0
            valueBinary = (evt.value == 'detected') ? '1i' : '0i'
            break
        case 'sleeping':
            // binary value: sleeping = 1, <any other value> = 0
            valueBinary = (evt.value == 'sleeping') ? '1i' : '0i'
            break
        case 'smoke':
            // binary value: detected = 1, <any other value> = 0
            valueBinary = (evt.value == 'detected') ? '1i' : '0i'
            break
        case 'sound':
            // binary value: detected = 1, <any other value> = 0
            valueBinary = (evt.value == 'detected') ? '1i' : '0i'
            break
        case 'switch':
            // binary value: on = 1, <any other value> = 0
            valueBinary = (evt.value == 'on') ? '1i' : '0i'
            break
        case 'tamper':
            // binary value: detected = 1, <any other value> = 0
            valueBinary = (evt.value == 'detected') ? '1i' : '0i'
            break
        case 'thermostatMode':
            // binary value: <any other value> = 1, off = 0
            valueBinary = (evt.value == 'off') ? '0i' : '1i'
            break
        case 'thermostatFanMode':
            // binary value: <any other value> = 1, auto = 0
            valueBinary = (evt.value == 'auto') ? '0i' : '1i'
            break
        case 'thermostatOperatingState':
            // binary value: heating or cooling = 1, <any other value> = 0
            valueBinary = (evt.value == 'heating' || evt.value == 'cooling') ? '1i' : '0i'
            break
        case 'thermostatSetpointMode':
            // binary value: followSchedule = 0, <any other value> = 1
            valueBinary = (evt.value == 'followSchedule') ? '0i' : '1i'
            break
        case 'threeAxis':
            // threeAxis: Format to x,y,z values
            unit = evt.name
            try {
                /* groovylint-disable-next-line UnusedVariable, VariableName */
                def (_,x,y,z) = (evt.value =~ /^\[x:(-?[0-9]{1,3}),y:(-?[0-9]{1,3}),z:(-?[0-9]{1,3})\]$/)[0]
                value = "valueX=${x}i,valueY=${y}i,valueZ=${z}i" // values are integers
            }
            catch (e) {
                value = "valueX=0i,valueY=0i,valueZ=0i"
                logger("Invalid threeAxis format: ${evt.value}", logWarn)
            }
            break
        case 'touch':
            // binary value: touched = 1, <any other value> = 0
            valueBinary = (evt.value == 'touched') ? '1i' : '0i'
            break
        case 'valve':
            // binary value: open = 1, <any other value> = 0
            valueBinary = (evt.value == 'open') ? '1i' : '0i'
            break
        case 'water':
            // binary value: wet = 1, <any other value> = 0
            valueBinary = (evt.value == 'wet') ? '1i' : '0i'
            break
        case 'windowShade':
            // binary value: closed = 1, <any other value> = 0
            valueBinary = (evt.value == 'closed') ? '1i' : '0i'
            break

        // The Mysterious Case of The Button
        // binary value: released = 0, <any other value> = 1
        // Note that button attributes are excluded from softpoll
        case 'doubleTapped':
        case 'held':
        case 'pushed':
            unit = 'button'
            valueBinary = '1i'
            break
        case 'released':
            unit = 'button'
            valueBinary = '0i'
            break
    }

    // If a unit tag has not been assigned above, assign it from the event.
    if (!unit) {
        if (valueBinary) {
            // If a binary value was set, use the event name as the unit tag.
            unit = escapeStringForInfluxDB(evt.name)
        }
        else if (evt.unit) {
            // Otherwise, use the event unit if defined.
            unit = escapeStringForInfluxDB(evt.unit)
        }
    }

    // If a value has not been assigned above, assign it from the event value.
    if (!value) {
        if (evt.value.isNumber()) {
            // It's a number. Common numerical events such as carbonDioxide, power, energy,
            // humidity, level, temperature, ultravioletIndex, voltage, etc. are handled here.
            value = evt.value
        }
        else {
            // Everything else is a string.
            value = '"' + escapeStringForInfluxDB(evt.value) + '"'
        }
    }

    // Build the data string to send to InfluxDB:
    //  Format: <measurement>[,<tag_name>=<tag_value>] field=<field_value>
    //    If value is an integer, it must have a trailing "i"
    //    If value is a string, it must be enclosed in double quotes.
    String measurement = escapeStringForInfluxDB((evt.name))
    String deviceId = evt.deviceId
    String deviceName = escapeStringForInfluxDB(evt.displayName)
    String data = "${measurement},deviceName=${deviceName},deviceId=${deviceId}"

    // Add hub name (location) tag if requested
    if (settings.includeHubInfo == null || settings.includeHubInfo) {
        String hubName = escapeStringForInfluxDB(location.name)
        data += ",hubName=${hubName}"
    }

    // Add the unit and value(s)
    if (unit) {
        data += ",unit=${unit}"
    }
    if (value ==~ /^value.*/) {
        // Assignment has already been done above (e.g. threeAxis)
        data += " ${value}"
    }
    else {
        data += " value=${value}"
    }
    if (valueBinary) {
        data += ",valueBinary=${valueBinary}"
    }

    // Add the event timestamp
    long eventTimestamp = evt.unixTime * 1e6 // milliseconds to nanoseconds
    data += " ${eventTimestamp}"

    // Return the completed string
    return(data)
}

/**
 *  encodeDeviceEvent(evt)
 *
 *  Builds data from variable polls or variable change events to send to InfluxDB.
 *   - Escapes and quotes string values.
 *   - Interprets binary variables to write to valueBinary
 **/
private String encodeVariableEvent(evt) {

    String variableName = evt.name.substring(9)

    String variableNameEscaped = escapeStringForInfluxDB(variableName)
    String measurement = ""
    String value = ""
    String valueBinary = ""

    if (booleanVariables.contains(variableName)) {
        measurement = "binaryVariable"
        value = evt.value
        valueBinary = (evt.value == "true") ? '1i' : '0i'
    }
    else if (numberVariables.contains(variableName)){
        measurement = "numberVariable"
        value = evt.value
    }
    else if (decimalVariables.contains(variableName)){
        measurement = "decimalVariable"
        value = evt.value
    }
    else if (stringVariables.contains(variableName)){
        measurement = "stringVariable"
        value = '"' + escapeStringForInfluxDB(evt.value) + '"'
    }
    else if (datetimeVariables.contains(variableName)){
        measurement = "datetimeVariable"
        String datetime = evt.value
        String[] datetimeSplitted = datetime.split(' ');
        value = '"' + escapeStringForInfluxDB(evt.value) + '"'
    }


    String data = "${measurement},variableName=${variableNameEscaped}"

    // Add hub name (location) tag if requested
    if (settings.includeHubInfo == null || settings.includeHubInfo) {
        String hubName = escapeStringForInfluxDB(location.name)
        data += ",hubName=${hubName}"
    }

    data += " value=${value}"

    if (valueBinary) {
        data += ",valueBinary=${valueBinary}"
    }

    // Add the event timestamp
    long eventTimestamp = evt.unixTime * 1e6 // milliseconds to nanoseconds
    data += " ${eventTimestamp}"

    // Return the completed string
    return(data)
}

/**
 *  handleEvent(evt)
 *
 *  Builds data to send to InfluxDB.
 *   - Escapes and quotes string values.
 *   - Calculates logical binary values where string values can be
 *     represented as binary values (e.g. contact: closed = 1, open = 0)
 **/
void handleEvent(evt) {
    logger("Handle Event: ${evt}", logDebug)

    // Encode the event
    data = encodeDeviceEvent(evt)

    // Add event to the queue for InfluxDB
    queueToInfluxDb([data])
}

/**
 *  handleVariableEvent(evt)
 *
 *  Build InfluxDB write string from variable change event.
 **/
void handleVariableEvent(evt) {
    logger("Handle Variable Event: ${evt}", logDebug)

    // Encode the event
    data = encodeVariableEvent(evt)

    // Add event to the queue for InfluxDB
    queueToInfluxDb([data])
}

/**
 *  encodeHubInfo(evt)
 *
 *  Build a Hub Information record.
 **/
private String encodeHubInfo(evt) {
    String hubName = escapeStringForInfluxDB(location.name)
    String localIP = escapeStringForInfluxDB(location.hub.localIP)
    String firmwareVersion = escapeStringForInfluxDB(location.hub.firmwareVersionString)
    String upTime = escapeStringForInfluxDB(location.hub.uptime.toString())
    String mode = escapeStringForInfluxDB(evt?.value ? evt.value : location.getMode())

    def times = getSunriseAndSunset()
    String sunriseTime = escapeStringForInfluxDB(times.sunrise.format("HH:mm", location.timeZone))
    String sunsetTime = escapeStringForInfluxDB(times.sunset.format("HH:mm", location.timeZone))

    Long eventTimestamp = (evt?.unixTime ? evt.unixTime : now()) * 1e6       // Time is in milliseconds, but InfluxDB expects nanoseconds

    String data = "_hubInfo,hubName=${hubName} localIP=\"${localIP}\",firmwareVersion=\"${firmwareVersion}\",upTime=\"${upTime}\",mode=\"${mode}\",sunriseTime=\"${sunriseTime}\",sunsetTime=\"${sunsetTime}\" ${eventTimestamp}"
    return data
}

/**
 *  handleModeEvent(evt)
 *
 *  Log hub information when mode changes.
 **/
void handleModeEvent(evt) {
    logger("Handle Mode Event: ${evt}", logDebug)

    // Encode the event
    data = encodeHubInfo(evt)

    // Add event to the queue for InfluxDB
    queueToInfluxDb([data])
}

/**
 *  softPoll()
 *
 *  Re-queues last value to InfluxDB unless an event has already been seen in the last softPollingInterval.
 *  Also calls LogSystemProperties().
 *
 *  NB: Function name softPoll must be kept for backward compatibility
 **/
void softPoll() {
    logger("Keepalive check", logDebug)

    // Migration: Old configurations will not have prefPostHubInfo set
    if (settings.prefPostHubInfo == null) {
        app.updateSetting("prefPostHubInfo", (Boolean) (settings.prefLogHubProperties || settings.prefLogLocationProperties || settings.prefLogModeEvents))
    }

    // Get the map
    Map<String,List> deviceAttrMap = getDeviceAttrMap()

    // Create the list
    Long timeNow = now()
    List<String> eventList = []
    deviceAttrMap.each { device, attrList ->
        attrList.each { attr ->
            if (momentaryAttributes.contains(attr)) {
                logger("Keep alive for device ${device}(${attr}) suppressed - momentary attribute", logDebug)
                return
            }
            if (device.latestState(attr)) {
                Integer activityMinutes = (timeNow - device.latestState(attr).date.time) / 60000
                if (activityMinutes > state.softPollingInterval) {
                    logger("Keep alive for device ${device}(${attr})", logDebug)
                    event = encodeDeviceEvent([
                        name: attr,
                        value: device.latestState(attr).value,
                        unit: device.latestState(attr).unit,
                        device: device,
                        deviceId: device.id,
                        displayName: device.displayName,
                        unixTime: timeNow
                    ])
                    eventList.add(event)
                }
                else {
                    logger("Keep alive for device ${device}(${attr}) unnecessary - last activity ${activityMinutes} minutes", logDebug)
                }
            }
            else {
                logger("Keep alive for device ${device}(${attr}) suppressed - last activity never", logDebug)
            }
        }
    }

    // Add a hub information record if requested
    if (settings.prefPostHubInfo) {
        eventList.add(encodeHubInfo(null))
    }

    eventList += getVariableEventList()

    // Queue the events
    queueToInfluxDb(eventList)
}

/**
 *  getVariableEventList()
 *
 *  Polls all variables on the hub, read the variables that InfluxDB logger is tracking, 
 *  and create events to be queued to write to InfluxDB.
 **/
List<String> getVariableEventList() {

    // Get all variables
    Map<String,Map<String,String>> variables = getAllGlobalVars()

    // Create the list
    Long timeNow = now()
    List<String> eventList = []

    (booleanVariables+numberVariables+decimalVariables+stringVariables+datetimeVariables).each { name ->
        Map<String, String> variable = variables[name]
        String event = encodeVariableEvent([
            name: "variable:" + name,
            value: variable.value,
            unixTime: timeNow
        ])
        eventList.add(event)
        logger("${variable.value}", logError)
    }

    // Return the events
    return(eventList)
}

/**
 *  queueToInfluxDb()
 *
 *  Adds events to the InfluxDB queue.
 **/
private void queueToInfluxDb(List<String> eventList) {
    if (state.loggerQueue == null) {
        // Failsafe if coming from an old version
        state.loggerQueue = []
    }

    // Add the data to the queue
    priorLoggerQueueSize = state.loggerQueue.size()
    state.loggerQueue += eventList
    eventList.each { event ->
        logger("Queued event: ${event}", logInfo)
    }

    // If this is the first data in the batch, trigger the timer
    if (priorLoggerQueueSize == 0) {
        logger("Scheduling batch", logDebug)
        // Migration: prefBatchTimeLimit does not exist in older configurations
        if (settings.prefBatchTimeLimit == null) {
            app.updateSetting("prefBatchTimeLimit", (Long) 60)
        }
        runIn(settings.prefBatchTimeLimit, writeQueuedDataToInfluxDb)
    }
}

/**
 *  writeQueuedDataToInfluxDb()
 *
 *  Posts data to InfluxDB queue.
 *
 *  NB: Function name writeQueuedDataToInfluxDb must be kept for backward compatibility
**/
void writeQueuedDataToInfluxDb() {
    if (state.loggerQueue == null) {
        // Failsafe if coming from an old version
        return
    }
    if (state.uri == null) {
        // Failsafe if using an old config
        setupDB()
    }

    Integer loggerQueueSize = state.loggerQueue.size()
    logger("Number of events queued for InfluxDB: ${loggerQueueSize}", logDebug)
    if (loggerQueueSize == 0) {
        return
    }

    // Migration: Old configurations will not have prefBacklogLimit or prefBatchSizeLimit set
    if (settings.prefBacklogLimit == null) {
        app.updateSetting("prefBacklogLimit", (Long) 5000)
    }
    if (settings.prefBatchSizeLimit == null) {
        app.updateSetting("prefBatchSizeLimit", (Long) 50)
    }

    // Migration: Transitioning from older versions will not have state.postCount set
    Integer postCount = state.postCount ?: 0
    Long timeNow = now()
    if (postCount) {
        // A post is already running
        Long elapsed = timeNow - state.lastPost
        logger("Post of ${postCount} events already running (elapsed ${elapsed}ms)", logDebug)
        if (elapsed < 90000) {
            // Come back later
            runIn(30, writeQueuedDataToInfluxDb)
            return
        }

        // Failsafe in case handleInfluxResponse doesn't get called for some reason such as reboot
        logger("Post callback failsafe timeout", logDebug)
        state.postCount = 0

        if (loggerQueueSize > settings.prefBacklogLimit) {
            logger("Backlog of ${state.loggerQueue.size()} events exceeds limit of ${settings.prefBacklogLimit}: dropping ${postCount} events (failsafe)", logError)
            state.loggerQueue = state.loggerQueue.drop(postCount)
            loggerQueueSize -= postCount
        }
    }

    // If we have a backlog, log a warning
    if (loggerQueueSize > settings.prefBacklogLimit) {
        logger("Backlog of ${loggerQueueSize} events queued for InfluxDB", logWarn)
    }

    postCount = loggerQueueSize < settings.prefBatchSizeLimit ? loggerQueueSize : settings.prefBatchSizeLimit
    state.postCount = postCount
    state.lastPost = timeNow

    String data = state.loggerQueue.subList(0, postCount).toArray().join('\n')
    // Uncommenting the following line will eventually drive your hub into the ground. Don't do it.
    // logger("Posting data to InfluxDB: ${state.uri}, Data: [${data}]", logDebug)

    // Post it
    def postParams = [
        uri: state.uri,
        requestContentType: 'application/json',
        contentType: 'application/json',
        headers: state.headers,
        ignoreSSLIssues: settings.prefIgnoreSSLIssues,
        timeout: 60,
        body: data
    ]
    asynchttpPost('handleInfluxResponse', postParams, [ postTime: timeNow ])
}

/**
 *  handleInfluxResponse()
 *
 *  Handles response from post made in writeQueuedDataToInfluxDb().
 *
 *  NB: Function name handleInfluxResponse must be kept for backward compatibility
 **/
void handleInfluxResponse(hubResponse, closure) {
    if (state.loggerQueue == null) {
        // Failsafe if coming from an old version
        return
    }

    // Migration: Transitioning from older versions will not have closure set
    Double elapsed = (closure) ? (now() - closure.postTime) / 1000 : 0
    // Migration: Transitioning from older versions will not have postCount set
    Integer postCount = state.postCount ?: 0
    state.postCount = 0

    if (hubResponse.status < 400) {
        logger("Post of ${postCount} events complete - elapsed time ${elapsed} seconds - Status: ${hubResponse.status}", logInfo)
    }
    else {
        logger("Post of ${postCount} events failed - elapsed time ${elapsed} seconds - Status: ${hubResponse.status}, Error: ${hubResponse.errorMessage}, Headers: ${hubResponse.headers}, Data: ${data}", logWarn)
        if (postCount == 1) {
            logger("Failed record was: ${state.loggerQueue[0]}", logError)
        }

        // Migration: Old configurations will not have prefBacklogLimit set
        if (settings.prefBacklogLimit == null) {
            app.updateSetting("prefBacklogLimit", (Long) 5000)
        }

        if (state.loggerQueue.size() <= settings.prefBacklogLimit) {
            if (state.loggerQueue.size() > postCount) {
                logger("Backlog of ${state.loggerQueue.size()} events", logWarn)
            }

            // Try again later
            runIn(60, writeQueuedDataToInfluxDb)
            return
        }

        logger("Backlog of ${state.loggerQueue.size()} events exceeds limit of ${settings.prefBacklogLimit}: dropping ${postCount} events", logError)
    }

    // Remove the post from the queue
    state.loggerQueue = state.loggerQueue.drop(postCount)

    // Go again?
    if (state.loggerQueue.size()) {
        runIn(1, writeQueuedDataToInfluxDb)
    }
}

/**
 *  uriString()
 *
 *  Format the uri string.
 **/
private String uriString() {
    String uri

    if (settings?.prefDatabaseTls) {
        uri = "https://"
    }
    else {
        uri = "http://"
    }

    uri += settings.prefDatabaseHost
    if (settings?.prefDatabasePort) {
        uri += ":" + settings.prefDatabasePort
    }

    if (settings?.prefInfluxVer == "2") {
        uri += "/api/v2/write?org=${settings.prefOrg}&bucket=${settings.prefBucket}"
    }
    else {
        // Influx version 1
        uri += "/write?db=${settings.prefDatabaseName}"
    }

    return uri
}

/**
 *  setupDB()
 *
 *  Set up the database uri and header state variables.
 **/
private void setupDB() {
    def headers = [:]
    if (settings.prefAuthType == null || settings.prefAuthType == "basic") {
        if (settings.prefDatabaseUser && settings.prefDatabasePass) {
            String userpass = "${settings.prefDatabaseUser}:${settings.prefDatabasePass}"
            headers.put("Authorization", "Basic " + userpass.bytes.encodeBase64())
        }
    }
    else if (settings.prefAuthType == "token") {
        headers.put("Authorization", "Token ${settings.prefDatabaseToken}")
    }

    state.uri = uriString()
    state.headers = headers

    logger("InfluxDB URI: ${state.uri}", logInfo)

    // Migration: Clean up old state vars if present
    state.remove("databaseHost")
    state.remove("databasePort")
    state.remove("databaseName")
    state.remove("databasePass")
    state.remove("databaseUser")
    state.remove("path")
}

/**
 *  logger()
 *
 *  Wrapper function for logging.
 **/
private void logger(String msg, Integer level = logDebug) {
    Integer loggingLevel = settings.configLoggingLevelIDE != null ? settings.configLoggingLevelIDE.toInteger() : logWarn
    if (level > loggingLevel) {
        return
    }

    switch (level) {
        case logError:
            log.error msg
            break
        case logWarn:
            log.warn msg
            break
        case logInfo:
            log.info msg
            break
        case logDebug:
            log.debug msg
            break
    }
}

/**
 *  escapeStringForInfluxDB()
 *
 *  Escape values to InfluxDB.
 *
 *  If a tag key, tag value, or field key contains a space, comma, or an equals sign = it must
 *  be escaped using the backslash character \. Backslash characters do not need to be escaped.
 *  Commas and spaces will also need to be escaped for measurements, though equals signs = do not.
 *
 *  Further info: https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_reference/
 **/
private String escapeStringForInfluxDB(String str) {
    if (str == null) {
        return 'null'
    }

    str = str.replaceAll(" ", "\\\\ ") // Escape spaces.
    str = str.replaceAll(",", "\\\\,") // Escape commas.
    str = str.replaceAll("=", "\\\\=") // Escape equal signs.
    str = str.replaceAll("\"", "\\\\\"") // Escape double quotes.

    return str
}
