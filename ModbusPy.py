# https://pypi.org/project/pymodbus/
#

import time
from tango import AttrQuality, AttrWriteType, DispLevel, DevState, Attr, CmdArgType, UserDefaultAttrProp
from tango.server import Device, attribute, command, DeviceMeta
from tango.server import class_property, device_property
from tango.server import run
import os
import json
from json import JSONDecodeError
import traceback
from pymodbus.client.sync import ModbusTcpClient, ModbusSerialClient


class ModbusPy(Device, metaclass=DeviceMeta):

    # ───────────── Device Properties ─────────────
    protocol = device_property(dtype=str, default_value="TCP")  # TCP | RTU

    # TCP
    host = device_property(dtype=str, default_value="127.0.0.1")
    port = device_property(dtype=int, default_value=502)

    # RTU
    serial_port = device_property(dtype=str, default_value="/dev/ttyUSB0")
    baudrate = device_property(dtype=int, default_value=9600)
    parity = device_property(dtype=str, default_value="N")
    stopbits = device_property(dtype=int, default_value=1)
    bytesize = device_property(dtype=int, default_value=8)

    init_dynamic_attributes = device_property(dtype=str, default_value="")

    # ───────────── Internal State ─────────────
    client = None
    dynamicAttributes = {}
    dynamicAttributeValueTypes = {}
    dynamicAttributeModbusLookup = {}

    # ───────────── Lifecycle ─────────────
    def init_device(self):
        self.set_state(DevState.INIT)
        self.get_device_properties(self.get_device_class())
        self.last_error = ""
        self.connect()

        if self.init_dynamic_attributes:
            try:
                attrs = json.loads(self.init_dynamic_attributes)
                for a in attrs:
                    self.add_dynamic_attribute(
                        a["name"],
                        a.get("data_type", ""),
                        a.get("min_value", ""),
                        a.get("max_value", ""),
                        a.get("unit", ""),
                        a.get("write_type", ""),
                        a.get("label", ""),
                        a.get("modifier", ""),
                        a.get("register", "")
                    )
            except Exception:
                for name in self.init_dynamic_attributes.split(","):
                    self.add_dynamic_attribute(name.strip())

        self.set_state(DevState.ON)

    # ───────────── Connection ─────────────
    def connect(self):
        try:
            if self.client:
                self.client.close()

            if self.protocol.upper() == "TCP":
                self.client = ModbusTcpClient(self.host, port=self.port)
            else:
                self.client = ModbusSerialClient(
                    method="rtu",
                    port=self.serial_port,
                    baudrate=self.baudrate,
                    parity=self.parity,
                    stopbits=self.stopbits,
                    bytesize=self.bytesize,
                    timeout=1
                )

            if not self.client.connect():
                raise RuntimeError("Modbus connection failed")

            self.info_stream("Connected to Modbus device")

        except Exception as e:
            self.last_error = str(e)
            self.error_stream(traceback.format_exc())
            self.set_state(DevState.FAULT)

    # ───────────── Dynamic Attributes ─────────────
    def add_dynamic_attribute(
        self, name, variable_type_name="DevString",
        min_value="", max_value="", unit="",
        write_type_name="", label="", modifier="", register=""
    ):
        if not name:
            return

        prop = UserDefaultAttrProp()
        var_type = self.stringValueToVarType(variable_type_name)
        write_type = self.stringValueToWriteType(write_type_name)

        if unit:
            prop.set_unit(unit)
        if label:
            prop.set_label(label)

        attr = Attr(name, var_type, write_type)
        attr.set_default_properties(prop)

        self.dynamicAttributeValueTypes[name] = var_type
        self.dynamicAttributeModbusLookup[name] = register
        self.dynamicAttributes[name] = 0

        self.add_attribute(attr,
            r_meth=self.read_dynamic_attr,
            w_meth=self.write_dynamic_attr
        )

    # ───────────── Attribute Access ─────────────
    def read_dynamic_attr(self, attr):
        name = attr.get_name()
        value = self.modbusRead(name)
        attr.set_value(self.castValue(name, value))

    def write_dynamic_attr(self, attr):
        name = attr.get_name()
        value = attr.get_write_value()
        self.modbusWrite(name, value)

    # ───────────── Modbus Logic ─────────────
    def parse_register(self, name):
        try:
            rtype, addr, count, unit = self.dynamicAttributeModbusLookup[name].split(",")
            return rtype.lower(), int(addr), int(count), int(unit)
        except Exception:
            raise ValueError(
                f"Invalid modifier for {name}, expected: type,address,count,unit"
            )

    def modbusRead(self, name):
        rtype, addr, count, unit = self.parse_register(name)

        if rtype == "holding":
            rr = self.client.read_holding_registers(addr, count, unit=unit)
            return rr.registers[0]

        if rtype == "input":
            rr = self.client.read_input_registers(addr, count, unit=unit)
            return rr.registers[0]

        if rtype == "coil":
            rr = self.client.read_coils(addr, count, unit=unit)
            return rr.bits[0]

        if rtype == "discrete":
            rr = self.client.read_discrete_inputs(addr, count, unit=unit)
            return rr.bits[0]

        raise ValueError(f"Unsupported register type: {rtype}")

    def modbusWrite(self, name, value):
        rtype, addr, _, unit = self.parse_register(name)

        if rtype == "holding":
            self.client.write_register(addr, int(value), unit=unit)
        elif rtype == "coil":
            self.client.write_coil(addr, bool(value), unit=unit)
        else:
            raise ValueError(f"Register type {rtype} is read-only")

    # ───────────── Type Helpers ─────────────
    def stringValueToVarType(self, name):
        return {
            "DevBoolean": CmdArgType.DevBoolean,
            "DevLong": CmdArgType.DevLong,
            "DevDouble": CmdArgType.DevDouble,
            "DevFloat": CmdArgType.DevFloat,
            "DevString": CmdArgType.DevString,
            "": CmdArgType.DevString
        }.get(name, CmdArgType.DevString)

    def stringValueToWriteType(self, name):
        return {
            "READ": AttrWriteType.READ,
            "WRITE": AttrWriteType.WRITE,
            "READ_WRITE": AttrWriteType.READ_WRITE,
            "": AttrWriteType.READ_WRITE
        }.get(name, AttrWriteType.READ_WRITE)

    def castValue(self, name, value):
        t = self.dynamicAttributeValueTypes[name]
        if t == CmdArgType.DevBoolean:
            return bool(value)
        if t == CmdArgType.DevLong:
            return int(value)
        if t in (CmdArgType.DevFloat, CmdArgType.DevDouble):
            return float(value)
        return str(value)

if __name__ == "__main__":
    run({os.getenv("DEVICE_SERVER_NAME"): ModbusPy})