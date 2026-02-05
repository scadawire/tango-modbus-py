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
import struct

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

        self.dynamicAttributeModbusLookup[name] = {
            "variableType": variableType,
            "register": self.parse_register(register),
        }

        self.dynamicAttributes[name] = 0

        self.add_attribute(attr,
            r_meth=self.read_dynamic_attr,
            w_meth=self.write_dynamic_attr
        )

    # ───────────── Attribute Access ─────────────
    def read_dynamic_attr(self, attr):
        name = attr.get_name()
        valueBinary = self.modbusRead(name)
        attr.set_value(self.bytedata_to_variable(valueBinary,
            self.dynamicAttributeModbusLookup[name]["variableType"],
            self.dynamicAttributeModbusLookup[name]["register"]["subaddr"]
        ))

    def write_dynamic_attr(self, attr):
        name = attr.get_name()
        value = attr.get_write_value()
        self.modbusWrite(name, self.variable_to_bytedata(value,
            self.dynamicAttributeModbusLookup[name]["variableType"],
            self.dynamicAttributeModbusLookup[name]["register"]["subaddr"]
        )

    # ───────────── Modbus Logic ─────────────
    def parse_register(self, register):
        try:
            parts = register.split(".")
            unit = parts[0]
            rtype = parts[1]
            rtype = rtype.lower()
            rtype = rtype.replace("h", "holding")
            rtype = rtype.replace("i", "input")
            rtype = rtype.replace("c", "coil")
            rtype = rtype.replace("d", "discrete")
            addr = parts[2]
            subaddress = parts[3] if len(parts) > 3 else 0
            return {
                "rtype": rtype,
                "addr": int(addr, 0),
                "subaddr" : int(subaddress)
                "unit": int(unit)
            }
        except Exception:
            raise ValueError(
                f"Invalid modifier for {register}, expected: unit.type.address[.subaddress]"
            )

    def modbusRead(self, name):
        register = self.dynamicAttributeModbusLookup[name]["register"]
        unit = register["unit"]
        rtype = register["rtype"]
        addr = register["addr"]
        count = self.bytes_per_variable_type(self.dynamicAttributeModbusLookup[name]["variableType"])

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
        register = self.dynamicAttributeModbusLookup[name]["register"]
        unit = register["unit"]
        rtype = register["rtype"]
        addr = register["addr"]

        if rtype == "holding":
            self.client.write_register(addr, value, unit=unit)
        elif rtype == "coil":
            self.client.write_coil(addr, value, unit=unit)
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

    def _apply_word_order(data, size):
        chunk = data[0:size]
        # Swap 16-bit words (Modbus-style)
        if self.word_order == "swapped" and size in (4, 8):
            half = size // 2
            return chunk[half:] + chunk[:half]
        return chunk

    def bytedata_to_variable(
        self,
        data,
        variableType,
        suboffset=0
    ):

        endian_prefix = ">" if self.endian == "little" else "<"

        if variableType == CmdArgType.DevFloat:
            # 32-bit IEEE float
            raw = _apply_word_order(data, 4)
            return struct.unpack(endian_prefix + "f", raw)[0]

        elif variableType == CmdArgType.DevDouble:
            # 64-bit IEEE double  ✅
            raw = _apply_word_order(data, 8)
            return struct.unpack(endian_prefix + "d", raw)[0]

        elif variableType == CmdArgType.DevLong:
            # 32-bit signed integer
            raw = _apply_word_order(data, 4)
            return struct.unpack(endian_prefix + "i", raw)[0]

        elif variableType == CmdArgType.DevBoolean:
            # Bit inside a byte
            byte_val = data[0]
            return bool((byte_val >> suboffset) & 0x01)

        elif variableType == CmdArgType.DevString:
            # Null-terminated string
            end = data.find(b"\x00", 0)
            if end == -1:
                end = len(data)
            return data[0:end].decode("utf-8", errors="ignore")

        else:
            raise Exception(f"unsupported variable type {variableType}")

    def _apply_word_order_encode(data):
        # data is already the correct size (4 or 8 bytes)
        if self.word_order == "swapped" and len(data) in (4, 8):
            half = len(data) // 2
            return data[half:] + data[:half]
        return data

    def variable_to_bytedata(
        self,
        value,
        variableType,
        suboffset=0
):
        """
        Writes value into data (bytearray) at offset
        """

        endian_prefix = ">" if self.endian == "little" else "<"

        if variableType == CmdArgType.DevFloat:
            raw = struct.pack(endian_prefix + "f", float(value))
            raw = _apply_word_order_encode(raw)
            return raw

        elif variableType == CmdArgType.DevDouble:
            raw = struct.pack(endian_prefix + "d", float(value))
            raw = _apply_word_order_encode(raw)
            return  raw

        elif variableType == CmdArgType.DevLong:
            raw = struct.pack(endian_prefix + "i", int(value))
            raw = _apply_word_order_encode(raw)
            return raw

        elif variableType == CmdArgType.DevBoolean:
            # Set/clear bit inside byte
            if value:
                valInt = 1 << suboffset
            else
                valInt = 0
            raw = struct.pack(endian_prefix + "i", int(valInt))
            return raw

        elif variableType == CmdArgType.DevString:
            encoded = str(value).encode("utf-8")
            return encoded

        else:
            raise Exception(f"unsupported variable type {variableType}")

    def bytes_per_variable_type(self, variableType):
        if(variableType == CmdArgType.DevShort):
            return 2
        if(variableType == CmdArgType.DevFloat):
            return 4
        elif(variableType == CmdArgType.DevDouble):
            return 8
        elif(variableType == CmdArgType.DevLong64):
            return 8
        elif(variableType == CmdArgType.DevLong): # 32bit int
            return 4
        elif(variableType == CmdArgType.DevBoolean): # attention! overrides full byte
            return 1
        return 0

if __name__ == "__main__":
    run({os.getenv("DEVICE_SERVER_NAME"): ModbusPy})