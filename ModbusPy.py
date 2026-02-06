# https://pypi.org/project/pymodbus/
#

import time
import os
import json
import struct
import traceback
from json import JSONDecodeError
from tango import AttrQuality, AttrWriteType, DispLevel, DevState, Attr, CmdArgType, UserDefaultAttrProp
from tango.server import Device, attribute, command, DeviceMeta, class_property, device_property, run
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

    endian = device_property(dtype=str, default_value="big")
    word_order = device_property(dtype=str, default_value="normal")

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
                        a.get("register", ""),
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
                    timeout=1,
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
            "variableType": var_type,
            "register": self.parse_register(register),
        }

        self.dynamicAttributes[name] = 0

        self.add_attribute(
            attr,
            r_meth=self.read_dynamic_attr,
            w_meth=self.write_dynamic_attr,
        )

    # ───────────── Attribute Access ─────────────
    def read_dynamic_attr(self, attr):
        name = attr.get_name()
        lookup = self.dynamicAttributeModbusLookup[name]
        raw_data = self.modbusRead(name)
        attr.set_value(
            self.bytedata_to_variable(
                raw_data,
                lookup["variableType"],
                lookup["register"]["subaddr"],
            )
        )

    def write_dynamic_attr(self, attr):
        name = attr.get_name()
        value = attr.get_write_value()
        lookup = self.dynamicAttributeModbusLookup[name]
        self.modbusWrite(
            name,
            self.variable_to_bytedata(
                value,
                lookup["variableType"],
                lookup["register"]["subaddr"],
            ),
        )

    # ───────────── Modbus Logic ─────────────
    _REGISTER_TYPE_MAP = {
        "h": "holding",
        "i": "input",
        "c": "coil",
        "d": "discrete",
        "holding": "holding",
        "input": "input",
        "coil": "coil",
        "discrete": "discrete",
    }

    def parse_register(self, register):
        try:
            parts = register.split(".")
            unit = parts[0]
            rtype = parts[1].lower()
            rtype = self._REGISTER_TYPE_MAP.get(rtype, rtype)
            addr = parts[2]
            subaddress = parts[3] if len(parts) > 3 else 0
            return {
                "rtype": rtype,
                "addr": int(addr, 0),
                "subaddr": int(subaddress),
                "unit": int(unit),
            }
        except Exception:
            raise ValueError(
                f"Invalid register descriptor '{register}', "
                f"expected: unit.type.address[.subaddress]"
            )

    def _registers_needed(self, variableType):
        """Return the number of 16-bit Modbus registers to read/write."""
        nbytes = self.bytes_per_variable_type(variableType)
        return max(1, nbytes // 2)

    def modbusRead(self, name):
        lookup = self.dynamicAttributeModbusLookup[name]
        register = lookup["register"]
        unit = register["unit"]
        rtype = register["rtype"]
        addr = register["addr"]
        var_type = lookup["variableType"]
        count = self._registers_needed(var_type)

        if rtype == "holding":
            rr = self.client.read_holding_registers(addr, count, unit=unit)
        elif rtype == "input":
            rr = self.client.read_input_registers(addr, count, unit=unit)
        elif rtype == "coil":
            rr = self.client.read_coils(addr, 1, unit=unit)
        elif rtype == "discrete":
            rr = self.client.read_discrete_inputs(addr, 1, unit=unit)
        else:
            raise ValueError(f"Unsupported register type: {rtype}")

        if rr.isError():
            raise RuntimeError(f"Modbus read error for '{name}': {rr}")

        # Coils / discrete → return raw bool
        if rtype in ("coil", "discrete"):
            return rr.bits[0]

        # Holding / input → convert register list to bytes for struct.unpack
        raw = b""
        for reg_val in rr.registers:
            raw += struct.pack(">H", reg_val)  # each register is big-endian 16-bit
        return raw

    def modbusWrite(self, name, value):
        register = self.dynamicAttributeModbusLookup[name]["register"]
        unit = register["unit"]
        rtype = register["rtype"]
        addr = register["addr"]

        if rtype == "holding":
            if isinstance(value, (bytes, bytearray)):
                # Convert byte string to list of 16-bit register values
                regs = []
                for i in range(0, len(value), 2):
                    regs.append(struct.unpack(">H", value[i:i + 2])[0])
                if len(regs) == 1:
                    self.client.write_register(addr, regs[0], unit=unit)
                else:
                    self.client.write_registers(addr, regs, unit=unit)
            else:
                self.client.write_register(addr, int(value), unit=unit)

        elif rtype == "coil":
            self.client.write_coil(addr, bool(value), unit=unit)
        else:
            raise ValueError(f"Register type '{rtype}' is read-only")

    # ───────────── Type Helpers ─────────────
    def stringValueToVarType(self, name):
        return {
            "DevBoolean": CmdArgType.DevBoolean,
            "DevLong": CmdArgType.DevLong,
            "DevDouble": CmdArgType.DevDouble,
            "DevFloat": CmdArgType.DevFloat,
            "DevString": CmdArgType.DevString,
            "": CmdArgType.DevString,
        }.get(name, CmdArgType.DevString)

    def stringValueToWriteType(self, name):
        return {
            "READ": AttrWriteType.READ,
            "WRITE": AttrWriteType.WRITE,
            "READ_WRITE": AttrWriteType.READ_WRITE,
            "": AttrWriteType.READ_WRITE,
        }.get(name, AttrWriteType.READ_WRITE)

    def _apply_word_order(self, data, size):
        chunk = data[0:size]
        if self.word_order == "swapped" and size in (4, 8):
            half = size // 2
            return chunk[half:] + chunk[:half]
        return chunk

    def bytedata_to_variable(self, data, variableType, suboffset=0):
        #   ">" means big-endian, "<" means little-endian.
        endian_prefix = ">" if self.endian == "big" else "<"

        if variableType == CmdArgType.DevFloat:
            raw = self._apply_word_order(data, 4)
            return struct.unpack(endian_prefix + "f", raw)[0]

        elif variableType == CmdArgType.DevDouble:
            raw = self._apply_word_order(data, 8)
            return struct.unpack(endian_prefix + "d", raw)[0]

        elif variableType == CmdArgType.DevLong:
            raw = self._apply_word_order(data, 4)
            return struct.unpack(endian_prefix + "i", raw)[0]

        elif variableType == CmdArgType.DevBoolean:
            if isinstance(data, bool):
                return data
            byte_val = data[0] if isinstance(data, (bytes, bytearray)) else int(data)
            return bool((byte_val >> suboffset) & 0x01)

        elif variableType == CmdArgType.DevString:
            if isinstance(data, (bytes, bytearray)):
                end = data.find(b"\x00", 0)
                if end == -1:
                    end = len(data)
                return data[0:end].decode("utf-8", errors="ignore")
            return str(data)

        else:
            raise Exception(f"Unsupported variable type: {variableType}")

    def _apply_word_order_encode(self, data):
        if self.word_order == "swapped" and len(data) in (4, 8):
            half = len(data) // 2
            return data[half:] + data[:half]
        return data

    def variable_to_bytedata(self, value, variableType, suboffset=0):
        endian_prefix = ">" if self.endian == "big" else "<"

        if variableType == CmdArgType.DevFloat:
            raw = struct.pack(endian_prefix + "f", float(value))
            return self._apply_word_order_encode(raw)

        elif variableType == CmdArgType.DevDouble:
            raw = struct.pack(endian_prefix + "d", float(value))
            return self._apply_word_order_encode(raw)

        elif variableType == CmdArgType.DevLong:
            raw = struct.pack(endian_prefix + "i", int(value))
            return self._apply_word_order_encode(raw)

        elif variableType == CmdArgType.DevBoolean:
            if value:
                valInt = 1 << suboffset
            else:
                valInt = 0
            raw = struct.pack(endian_prefix + "i", int(valInt))
            return raw

        elif variableType == CmdArgType.DevString:
            return str(value).encode("utf-8")

        else:
            raise Exception(f"Unsupported variable type: {variableType}")

    def bytes_per_variable_type(self, variableType):
        if variableType == CmdArgType.DevShort:
            return 2
        if variableType == CmdArgType.DevFloat:
            return 4
        elif variableType == CmdArgType.DevDouble:
            return 8
        elif variableType == CmdArgType.DevLong64:
            return 8
        elif variableType == CmdArgType.DevLong:
            return 4
        elif variableType == CmdArgType.DevBoolean:
            return 1
        return 0

if __name__ == "__main__":
    server_name = os.getenv("DEVICE_SERVER_NAME", "ModbusPy")
    run({server_name: ModbusPy})
