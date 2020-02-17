package me.biocomp.hubitat_ci_example

import me.biocomp.hubitat_ci.api.Attribute
import me.biocomp.hubitat_ci.api.State
import me.biocomp.hubitat_ci.api.app_api.AppExecutor
import me.biocomp.hubitat_ci.api.common_api.DeviceWrapper
import me.biocomp.hubitat_ci.api.common_api.Event
import me.biocomp.hubitat_ci.api.common_api.Hub
import me.biocomp.hubitat_ci.api.common_api.InstalledAppWrapper
import me.biocomp.hubitat_ci.api.common_api.Location
import me.biocomp.hubitat_ci.api.common_api.Log
import me.biocomp.hubitat_ci.app.HubitatAppSandbox
import me.biocomp.hubitat_ci.validation.Flags
import spock.lang.Specification
import spock.lang.Unroll

class Test extends
        Specification
{
    HubitatAppSandbox sandbox = new HubitatAppSandbox(new File("influxdb-logger.groovy"))

    def "Basic validation"() {
        expect:
            sandbox.run()
    }

    private def log = Mock(Log)
    private def app = Mock(InstalledAppWrapper){
        _*getLabel() >> "My label"
    }

    private def state = [:]

    private AppExecutor api = Mock{
        _*getLog() >> log
        _*getState() >> state
        _*getApp() >> app
        _*getLocation() >> Mock(Location)
    }

    private void updateStateToForceHttpPost() {
        // Queue enough data to trigger immediate scheduling of httpPost to database
        final int minElementsToBatch = 100
        for (int i = 0; i != minElementsToBatch; ++i) {
            state.queuedData.add("MockData")
        }
    }

    def "Authorisation properly printed into headers"() {
        when:
            // Parse, construct script object, run validations
            def script = sandbox.run(
                    api: api,
                    userSettingValues: [
                            prefSoftPollingInterval: "10",
                            prefDatabaseUser: "MyUserName",
                            prefDatabasePass: "MyPassword"],
                    validationFlags: [
                            Flags.AllowAnyExistingDeviceAttributeOrCapabilityInSubscribe])

            script.installed()
            script.updated()

            updateStateToForceHttpPost()

            // Send event (using Mock(Event) instead of map to get more precise event interface validation)
            script.handleEvent(Mock(Event){
                _*getName() >> "power"
                _*getValue() >> 123
                _*getUnit() >> "W"
                _*getDevice() >> Mock(DeviceWrapper){
                    _*getHub() >> Mock(Hub){
                        _*getId() >> 789
                        _*getName() >> "My name"
                    }
                }
                _*getDeviceId() >> 456
                _*getDisplayName() >> "Event display name"
            })

        then:
            1*api.asynchttpPost("handleInfluxResponse", {
                it.headers.Authorization == "Basic " + "MyUserName:MyPassword".bytes.encodeBase64().toString()
            })
            state.headers.Authorization == "Basic " + "MyUserName:MyPassword".bytes.encodeBase64().toString()
    }

    def "After installed() and updated() state contains correct data, and proper events are scheduled/subscribed to"() {
        given:
            def temp1 = Mock(DeviceWrapper)
            def temp2 = Mock(DeviceWrapper)
            def powerMeter = Mock(DeviceWrapper)

        when:
            // Parse, construct script object, run validations
            def script = sandbox.run(
                    api: api,
                    userSettingValues: [
                            prefDatabaseName: "MyDb",
                            prefSoftPollingInterval: "444",
                            writeInterval: 5,
                            prefLogModeEvents: true,
                            powerMeters: [powerMeter],
                            temperatures: [temp1, temp2]],
                    validationFlags: [
                            Flags.AllowAnyExistingDeviceAttributeOrCapabilityInSubscribe])

            script.installed()
            script.updated()

        then:
            state.loggingLevelIDE == 3
            state.path == "/write?db=MyDb"
            !state.headers.size() != 0 // The rest is validated separately

            state.deviceAttributes.size() == 40
            state.deviceAttributes[7] == [ devices: 'colors', attributes: ['hue','saturation','color']]
            state.deviceAttributes[39] == [ devices: 'windowShades', attributes: ['windowShade']]

            state.softPollingInterval == 444
            state.writeInterval == 5

            // managing schedules...
            1*api.unschedule("softPoll")
            1*api.unschedule("writeQueuedDataToInfluxDb")
            1*api.schedule({it.contains("0/444 * * * ?")}, "softPoll")
            1*api.runEvery5Minutes("writeQueuedDataToInfluxDb")

            // managing subscriptions...
            1*api.unsubscribe()
            1*api.subscribe(_, "mode", "handleModeEvent")
            1*api.subscribe([powerMeter], "power", "handleEvent")
            1*api.subscribe([powerMeter], "voltage", "handleEvent")
            1*api.subscribe([powerMeter], "current", "handleEvent")
            1*api.subscribe([powerMeter], "powerFactor", "handleEvent")
            1*api.subscribe([temp1, temp2], "temperature", "handleEvent")
    }

    @Unroll
    def "String '#src' should be escaped as '#res'"() {
        expect:
            // Parse, construct script object, run validations
            res == sandbox.run(api: api).escapeStringForInfluxDB(src)

        where:
            src | res
            ""  | "null"
            null  | "null"
            "a" | "a"
            " " | "\\ "
            "spaces  are escaped" | "spaces\\ \\ are\\ escaped"
            "commas,,are,escaped" | "commas\\,\\,are\\,escaped"
            "double\"\"quotes\"are\"escaped\"" | "double\\\"\\\"quotes\\\"are\\\"escaped\\\""
            "==equal=sign=is==escaped" | "\\=\\=equal\\=sign\\=is\\=\\=escaped"
    }

    def "softPoll() logs system properties and calls handleEvent() for all devices and attributes"() {
        setup:
            int systemPropertiesCalled = 0
            def handleEventCalled = [:]

            def temp1 = Mock(DeviceWrapper){
                _*getId() >> "Temp1DeviceId"
                _*getDisplayName() >> "Temp1 display name"
                _*hasAttribute("temperature") >> true
                _*latestState("temperature") >> Mock(State){ _*getValue() >> 11; _*getUnit() >> "C" }
            }

            def temp2 = Mock(DeviceWrapper){
                _*getId() >> "Temp2DeviceId"
                _*getDisplayName() >> "Temp2 display name"
                _*hasAttribute("temperature") >> true
                _*latestState("temperature") >> Mock(State){ _*getValue() >> 22; _*getUnit() >> null }
            }

            def powerMeter = Mock(DeviceWrapper){
                _*getId() >> "PowerMeterDeviceId"
                _*getDisplayName() >> "PowerMeter display name"
                _*hasAttribute("power") >> true
                _*latestState("power") >> Mock(State){ _*getValue() >> 42; _*getUnit() >> "kW" }
            }

            def script = sandbox.run(
                    api: api,
                    validationFlags: [Flags.DontValidateSubscriptions],
                    userSettingValues: [
                        powerMeters: [powerMeter],
                        temperatures: [temp1, temp2]
                    ],
                    customizeScriptBeforeRun: {
                        script->
                            script.getMetaClass().logSystemProperties = { systemPropertiesCalled++ }
                            script.getMetaClass().handleEvent = {
                                evt ->
                                    handleEventCalled."${evt.deviceId}.${evt.name}" = evt
                            }
            })

            script.installed()
            script.updated()

        when:
            script.softPoll()

        then:
            systemPropertiesCalled == 1
            handleEventCalled == [
                    "PowerMeterDeviceId.power": [
                            name: "power",
                            value: "42",
                            unit: "kW",
                            device: powerMeter,
                            deviceId: "PowerMeterDeviceId",
                            displayName: "PowerMeter display name"
                    ],
                    "Temp1DeviceId.temperature": [
                        name: "temperature",
                        value: "11",
                        unit: "C",
                        device: temp1,
                        deviceId: "Temp1DeviceId",
                        displayName: "Temp1 display name"
                    ],
                    "Temp2DeviceId.temperature": [
                            name: "temperature",
                            value: "22",
                            unit: null,
                            device: temp2,
                            deviceId: "Temp2DeviceId",
                            displayName: "Temp2 display name"
                    ]
            ]
    }
}