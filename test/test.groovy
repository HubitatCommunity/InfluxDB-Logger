package me.biocomp.hubitat_ci_example

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
}