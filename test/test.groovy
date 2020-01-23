package me.biocomp.hubitat_ci_example

import me.biocomp.hubitat_ci.api.app_api.AppExecutor
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

    def "Authorisation properly printed into headers"() {
        setup:
            // Create mock log
            def log = Mock(Log)
            def app = Mock(InstalledAppWrapper){
                _*getLabel() >> "My label"
            }

            def state = [:]
            // Make AppExecutor return the mock log
            AppExecutor api = Mock{
                _*getLog() >> log
                _*getState() >> state
                _*getApp() >> app
                _*getLocation() >> Mock(Location)
            }

            // Parse, construct script object, run validations
            def script = sandbox.run(
                    api: api,
                    userSettingValues: [
                            prefSoftPollingInterval: "10",
                            prefDatabaseUser: "MyUserName",
                            prefDatabasePass: "MyPassword"],
                    validationFlags: [Flags.AllowAnyExistingDeviceAttributeOrCapabilityInSubscribe])

            script.installed()
            script.updated()

        expect:
            state.headers.Authorization == "Basic " + "MyUserName:MyPassword".bytes.encodeBase64().toString()
    }
}