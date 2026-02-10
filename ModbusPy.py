# https://pypi.org/project/pymodbus/
#

import time
import os
import json
import struct
import traceback
from json import JSONDecodeError
from tango import AttrQuality, AttrWriteType, AttrDataFormat, DispLevel, DevState
from tango import Attr, SpectrumAttr, ImageAttr, CmdArgType, UserDefaultAttrProp
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
                        a.get("data_format", ""),
                        str(a.get("max_x", "")),
                        str(a.get("max_y", "")),
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
        write_type_name="", label="", modifier="", register="",
        data_format_name="", max_x="", max_y=""
    ):
        if not name:
            return

        prop = UserDefaultAttrProp()
        var_type = self.stringValueToVarType(variable_type_name)
        write_type = self.stringValueToWriteType(write_type_name)
        data_format = self.stringValueToFormatType(data_format_name)
        dim_x = int(max_x) if max_x else 256
        dim_y = int(max_y) if max_y else 256

        if unit:
            prop.set_unit(unit)
        if label:
            prop.set_label(label)

        if data_format == AttrDataFormat.SPECTRUM:
            attr = SpectrumAttr(name, var_type, write_type, dim_x)
        elif data_format == AttrDataFormat.IMAGE:
            attr = ImageAttr(name, var_type, write_type, dim_x, dim_y)
        else:
            attr = Attr(name, var_type, write_type)
        attr.set_default_properties(prop)

        self.dynamicAttributeModbusLookup[name] = {
            "variableType": var_type,
            "register": self.parse_register(register),
            "dataFormat": data_format,
            "max_x": dim_x,
            "max_y": dim_y,
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
        data_format = lookup.get("dataFormat", AttrDataFormat.SCALAR)

        if data_format == AttrDataFormat.SPECTRUM:
            value = self.bytedata_to_array(raw_data, lookup["variableType"], lookup["max_x"])
        elif data_format == AttrDataFormat.IMAGE:
            value = self.bytedata_to_image(raw_data, lookup["variableType"], lookup["max_x"], lookup["max_y"])
        else:
            value = self.bytedata_to_variable(raw_data, lookup["variableType"], lookup["register"]["subaddr"])

        attr.set_value(value)

    def write_dynamic_attr(self, attr):
        name = attr.get_name()
        value = attr.get_write_value()
        lookup = self.dynamicAttributeModbusLookup[name]
        data_format = lookup.get("dataFormat", AttrDataFormat.SCALAR)

        if data_format == AttrDataFormat.SPECTRUM:
            self.modbusWrite(name, self.array_to_bytedata(value, lookup["variableType"]))
        elif data_format == AttrDataFormat.IMAGE:
            flat = [v for row in value for v in row]
            self.modbusWrite(name, self.array_to_bytedata(flat, lookup["variableType"]))
        elif lookup["variableType"] == CmdArgType.DevBoolean and lookup["register"]["rtype"] == "holding":
            self.modbusWriteBooleanBit(name, value)
        else:
            self.modbusWrite(
                name,
                self.variable_to_bytedata(value, lookup["variableType"], lookup["register"]["subaddr"]),
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

    def _registers_needed(self, variableType, subaddr, data_format=AttrDataFormat.SCALAR, max_x=0, max_y=0):
        if variableType == CmdArgType.DevBoolean and data_format in (AttrDataFormat.SPECTRUM, AttrDataFormat.IMAGE):
            total_bits = max_x if data_format == AttrDataFormat.SPECTRUM else max_x * max_y
            return max(1, (total_bits + 15) // 16)
        nbytes = self.bytes_per_variable_type(variableType)
        if variableType == CmdArgType.DevString:
            nbytes = subaddr
        elif data_format == AttrDataFormat.SPECTRUM:
            nbytes = nbytes * max_x
        elif data_format == AttrDataFormat.IMAGE:
            nbytes = nbytes * max_x * max_y
        return max(1, nbytes // 2)

    def modbusRead(self, name):
        lookup = self.dynamicAttributeModbusLookup[name]
        register = lookup["register"]
        unit = register["unit"]
        rtype = register["rtype"]
        addr = register["addr"]
        var_type = lookup["variableType"]
        data_format = lookup.get("dataFormat", AttrDataFormat.SCALAR)
        count = self._registers_needed(
            var_type, register["subaddr"],
            data_format, lookup.get("max_x", 0), lookup.get("max_y", 0),
        )

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
            return bool(rr.bits[0])

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
            if isinstance(value, (bytes, bytearray)):
                value = any(b != 0 for b in value)
            self.client.write_coil(addr, bool(value), unit=unit)
        else:
            raise ValueError(f"Register type '{rtype}' is read-only")

    def modbusWriteBooleanBit(self, name, value):
        """Read-modify-write a single bit in a holding register."""
        register = self.dynamicAttributeModbusLookup[name]["register"]
        unit = register["unit"]
        addr = register["addr"]
        bit_index = register["subaddr"]

        rr = self.client.read_holding_registers(addr, 1, unit=unit)
        if rr.isError():
            raise RuntimeError(f"Modbus read error for '{name}': {rr}")
        current_val = rr.registers[0]

        if bool(value):
            current_val |= (1 << bit_index)
        else:
            current_val &= ~(1 << bit_index)

        self.client.write_register(addr, current_val, unit=unit)

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

    def stringValueToFormatType(self, name):
        return {
            "SCALAR": AttrDataFormat.SCALAR,
            "SPECTRUM": AttrDataFormat.SPECTRUM,
            "IMAGE": AttrDataFormat.IMAGE,
        }.get(name, AttrDataFormat.SCALAR)

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
            if isinstance(data, (bytes, bytearray)):
                val = struct.unpack(endian_prefix + "H", data[:2])[0]
                return bool((val >> suboffset) & 0x01)
            return bool((int(data) >> suboffset) & 0x01)

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
            raw = struct.pack(endian_prefix + "H", int(valInt))
            return raw

        elif variableType == CmdArgType.DevString:
            raw = str(value).encode("utf-8")
            padding_needed = suboffset - len(raw)
            if padding_needed <= 0:
                raise Exception(f"String to long to fit into registers")
            raw += b"\x00" * padding_needed
            # pad to even length for 16-bit register alignment
            if len(raw) % 2 != 0: raw += b"\x00"
            return raw

        else:
            raise Exception(f"Unsupported variable type: {variableType}")

    def bytedata_to_array(self, data, variableType, count):
        if variableType == CmdArgType.DevBoolean:
            endian_prefix = ">" if self.endian == "big" else "<"
            result = []
            for i in range(count):
                reg_index = i // 16
                bit_index = i % 16
                reg_val = struct.unpack(endian_prefix + "H", data[reg_index * 2 : reg_index * 2 + 2])[0]
                result.append(bool((reg_val >> bit_index) & 0x01))
            return result
        elem_size = self.bytes_per_variable_type(variableType)
        result = []
        for i in range(count):
            chunk = data[i * elem_size : (i + 1) * elem_size]
            result.append(self.bytedata_to_variable(chunk, variableType))
        return result

    def bytedata_to_image(self, data, variableType, max_x, max_y):
        flat = self.bytedata_to_array(data, variableType, max_x * max_y)
        return [flat[r * max_x : (r + 1) * max_x] for r in range(max_y)]

    def array_to_bytedata(self, values, variableType):
        if variableType == CmdArgType.DevBoolean:
            endian_prefix = ">" if self.endian == "big" else "<"
            num_regs = (len(values) + 15) // 16
            regs = [0] * num_regs
            for i, val in enumerate(values):
                if bool(val):
                    regs[i // 16] |= (1 << (i % 16))
            result = b""
            for reg in regs:
                result += struct.pack(endian_prefix + "H", reg)
            return result
        result = b""
        for val in values:
            result += self.variable_to_bytedata(val, variableType)
        return result

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
            return 2
        return 0

if __name__ == "__main__":
    server_name = os.getenv("DEVICE_SERVER_NAME", "ModbusPy")
    run({server_name: ModbusPy})
