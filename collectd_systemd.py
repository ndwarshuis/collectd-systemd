import dbus
import collectd


class SystemD(object):
    def __init__(self):
        self.plugin_name = 'systemd'
        self.interval = 60.0
        self.verbose_logging = False
        self.services = []
        self.units = {}

    def log_verbose(self, msg):
        if not self.verbose_logging:
            return
        collectd.info('{} plugin [verbose]: {}'.format(self.plugin_name, msg))

    def init_dbus(self):
        self.units = {}
        self.bus = dbus.SystemBus()
        self.manager = dbus.Interface(self.bus.get_object('org.freedesktop.systemd1',
                                                          '/org/freedesktop/systemd1'),
                                      'org.freedesktop.systemd1.Manager')

    def get_unit(self, name):
        if name not in self.units:
            try:
                unit = dbus.Interface(self.bus.get_object('org.freedesktop.systemd1',
                                                          self.manager.GetUnit(name)),
                                      'org.freedesktop.DBus.Properties')
            except dbus.exceptions.DBusException as e:
                collectd.warning('{} plugin: failed to monitor unit {}: {}'.format(
                    self.plugin_name, name, e))
                return
            self.units[name] = unit
        return self.units[name]

    def get_service_state(self, name, prop, failval):
        unit = self.get_unit(name)
        if not unit:
            return failval
        else:
            try:
                return unit.Get('org.freedesktop.systemd1.Unit', prop)
            except dbus.exceptions.DBusException as e:
                self.log_verbose('{} plugin: failed to monitor unit {}: {}'.format(self.plugin_name, name, e))
                return failval

    def get_service_substate(self, name):
        return self.get_service_state(name, "SubState", "broken")

    def get_service_activestate(self, name):
        return self.get_service_state(name, "ActiveState", "failed")

    def configure_callback(self, conf):
        for node in conf.children:
            vals = [str(v) for v in node.values]
            if node.key == 'Service':
                self.services.extend(vals)
            elif node.key == 'Interval':
                self.interval = float(vals[0])
            elif node.key == 'Verbose':
                self.verbose_logging = (vals[0].lower() == 'true')
            else:
                raise ValueError('{} plugin: Unknown config key: {}'
                                 .format(self.plugin_name, node.key))
        if not self.services:
            self.log_verbose('No services defined in configuration')
            return
        self.init_dbus()
        collectd.register_read(self.read_callback, self.interval)
        self.log_verbose('Configured with services={}, interval={}'
                         .format(self.services, self.interval))

    def read_callback(self):
        self.log_verbose('Read callback called')
        for name in self.services:
            full_name = name + '.service'

            substate = self.get_service_substate(full_name)
            if substate == 'broken':
                self.log_verbose ('Unit {0} reported as broken. Reinitializing the connection to dbus & retrying.'.format(full_name))
                self.init_dbus()
                substate = self.get_service_substate(full_name)

            # send substate value
            subvalue = (1.0 if substate == 'running' or substate == 'reload' else 0.0)
            self.log_verbose('Sending value: {}.{}={} (substate={})'
                             .format(self.plugin_name, name, subvalue, substate))
            subRecord = collectd.Values(
                type='gauge',
                plugin=self.plugin_name,
                plugin_instance=name,
                type_instance='running',
                values=[subvalue])
            subRecord.dispatch()

            # send activestate value
            activestate = self.get_service_activestate(full_name)
            activevalue = (1.0 if activestate == 'active' else 0.0)
            self.log_verbose('Sending value: {}.{}={} (activestate={})'
                             .format(self.plugin_name, name, activevalue, activestate))
            activeRecord = collectd.Values(
                type='gauge',
                plugin=self.plugin_name,
                plugin_instance=name,
                type_instance='active',
                values=[activevalue])
            activeRecord.dispatch()


mon = SystemD()
collectd.register_config(mon.configure_callback)
