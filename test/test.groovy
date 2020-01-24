package me.biocomp.hubitat_ci_example

import me.biocomp.hubitat_ci.api.app_api.AppExecutor
import me.biocomp.hubitat_ci.api.common_api.DeviceWrapper
import me.biocomp.hubitat_ci.api.common_api.Event
import me.biocomp.hubitat_ci.api.common_api.Hub
import me.biocomp.hubitat_ci.api.common_api.InstalledAppWrapper
import me.biocomp.hubitat_ci.api.common_api.Location
import me.biocomp.hubitat_ci.api.common_api.Log
import me.biocomp.hubitat_ci.app.HubitatAppSandbox
import me.biocomp.hubitat_ci.app.HubitatAppScript
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

    private void initScript(HubitatAppScript script) {
        script.installed()
        script.updated()
        updateStateToForceHttpPost()
    }

    private void handleEvent(HubitatAppScript script, String name, Number value, String unit) {
        // Send event (using Mock(Event) instead of map to get more precise event interface validation)
        script.handleEvent(Mock(Event){
            _*getName() >> name
            _*getValue() >> value
            _*getUnit() >> unit
            _*getDevice() >> Mock(DeviceWrapper){
                _*getHub() >> Mock(Hub){
                    _*getId() >> 789
                    _*getName() >> "My name"
                }
            }
            _*getDeviceId() >> 456
            _*getDisplayName() >> "Event display name"
        })
    }

    def "Authorisation, database name, port and host are properly set in http post query"() {
        when:
            // Parse, construct script object, run validations
            def script = sandbox.run(
                    api: api,
                    userSettingValues: [
                            prefDatabaseHost: "MyHost",
                            prefDatabasePort: "1234",
                            prefDatabaseName: "MyDatabase",
                            prefDatabaseUser: "MyUserName",
                            prefDatabasePass: "MyPassword"],
                    validationFlags: [
                            Flags.AllowAnyExistingDeviceAttributeOrCapabilityInSubscribe])

            initScript(script)
            handleEvent(script, "power", 123, "W")

        then:
            1*api.asynchttpPost("handleInfluxResponse", {
                it.uri == "http://MyHost:1234" &&
                it.contentType == 'application/json' &&
                it.path == "/write" &&
                it.query == [ "db" : "MyDatabase" ] &&
                it.requestContentType == 'application/json' &&
                it.contentType == 'application/json' &&
                it.headers != null &&
                it.headers.Authorization == "Basic " + "MyUserName:MyPassword".bytes.encodeBase64().toString()
            })
            state.headers.Authorization == "Basic " + "MyUserName:MyPassword".bytes.encodeBase64().toString()
    }

    @Unroll
    def "Authorisation header is not set if #whatIsMissing is missing #userSettings"() {
        when:
            // Parse, construct script object, run validations
            def script = sandbox.run(
                    api: api,
                    userSettingValues: userSettings,
                    validationFlags: [
                            Flags.AllowAnyExistingDeviceAttributeOrCapabilityInSubscribe])

            initScript(script)
            handleEvent(script, "power", 123, "W")

        then:
            1*api.asynchttpPost("handleInfluxResponse", {
                it.path == "/write" &&
                it.requestContentType == 'application/json' &&
                it.contentType == 'application/json' &&
                (it.headers == null || it.headers.Authorization == null)
            })
            state.headers.Authorization == null

        where:
            whatIsMissing | userSettings
            "username" | [prefDatabaseUser: null, prefDatabasePass: "MyPassword"]
            "password" | [prefDatabaseUser: "MyUserName", prefDatabasePass: null]
    }
}