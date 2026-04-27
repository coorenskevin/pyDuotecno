from __future__ import annotations
from typing import Callable, Awaitable
import asyncio
import logging

from .protocol import NodeType, EV_NODEDATABASEINFO_2, BaseMessage
from .unit import (
    BaseUnit,
    SwitchUnit,
    SensUnit,
    DimUnit,
    DuoswitchUnit,
    VirtualUnit,
    ControlUnit,
)


class Node:
    installation_id: str
    name: str
    index: int
    nodeType: NodeType
    address: int
    numUnits: int
    physicalAddress: str | None
    softwareVersion: str | None
    units: dict[int, BaseUnit]
    ignoredUnits: set[int]
    isLoaded: asyncio.Event

    def __init__(
        self,
        name: str,
        installation_id: str,
        address: int,
        index: int,
        nodeType: NodeType,
        numUnits: int,
        physicalAddress: str | None,
        softwareVersion: str | None,
        writer: Callable[[str], Awaitable[None]],
        pwaiter: Callable[[str, str | None], Awaitable[None]],
    ) -> None:
        self._log = logging.getLogger("pyduotecno-node")
        self.installation_id = installation_id
        self.name = name
        self.address = address
        self.index = index
        self.numUnits = numUnits
        self.nodeType = nodeType
        self.physicalAddress = physicalAddress
        self.softwareVersion = softwareVersion
        self.writer = writer
        self.pwaiter = pwaiter
        self.isLoaded = asyncio.Event()
        self.isLoaded.clear()
        self.units = {}
        self.ignoredUnits = set()
        self._log.info(f"New node found: {self.name}")

    async def enable(self) -> None:
        for unit in self.units.values():
            await unit.enable()

    async def disable(self) -> None:
        for unit in self.units.values():
            await unit.disable()

    def get_name(self) -> str:
        return self.name

    def get_installation_id(self) -> str:
        return self.installation_id

    def get_address(self) -> int:
        return self.address

    def get_physical_address(self) -> str | None:
        return self.physicalAddress

    def get_software_version(self) -> str | None:
        return self.softwareVersion

    def __repr__(self) -> str:
        items = []
        for k, v in self.__dict__.items():
            if k not in ["_log", "writer"]:
                items.append(f"{k} = {v!r}")
        return "{}[{}]".format(type(self), ", ".join(items))

    def get_units(self) -> list[BaseUnit]:
        return list(self.units.values())

    def get_unit_by_type(self, unit_type: list[str] | str) -> list[BaseUnit]:
        if isinstance(unit_type, str):
            unit_type = [unit_type]
        wanted = set(unit_type)
        return [unit for unit in self.units.values() if type(unit).__name__ in wanted]

    def _map_protocol_unit_type(self, unit_type_name: str) -> type[BaseUnit]:
        unit_type_map: dict[str, type[BaseUnit]] = {
            "SWITCH": SwitchUnit,
            "SENS": SensUnit,
            "DIM": DimUnit,
            "DUOSWITCH": DuoswitchUnit,
            "VIRTUAL": VirtualUnit,
            "CONTROL": ControlUnit,
        }
        unit_class = unit_type_map.get(unit_type_name)
        if unit_class is None:
            self._log.warning(f"Unhandled unitType: {unit_type_name}")
            return BaseUnit
        return unit_class

    async def load(self) -> None:
        self._log.debug(f"Node {self.name}: Requesting units")
        for i in range(self.numUnits):
            await self.pwaiter(
                f"64,2,{self.address},{i}",
                f"[209,2,{self.address},{i}]",
            )

    async def handlePacket(self, packet: BaseMessage) -> None:
        if isinstance(packet, EV_NODEDATABASEINFO_2):
            if packet.unitName.startswith(","):
                self.ignoredUnits.add(packet.unit)
                if len(self.units) + len(self.ignoredUnits) == self.numUnits:
                    self.isLoaded.set()
                return
            if packet.unitTypeName in {"AUDIO_EXT", "AUDIO_BASIC", "AVMATRIC", "IRTX"}:
                self.ignoredUnits.add(packet.unit)
                if len(self.units) + len(self.ignoredUnits) == self.numUnits:
                    self.isLoaded.set()
                return
            if packet.unit not in self.units:
                u = self._map_protocol_unit_type(packet.unitTypeName)
                self.units[packet.unit] = u(
                    self, name=packet.unitName, unit=packet.unit,device_class="", writer=self.writer
                )
            if len(self.units) + len(self.ignoredUnits) == self.numUnits:
                self.isLoaded.set()
            return
        if hasattr(packet, "unit") and packet.unit in self.units:
            await self.units[packet.unit].handlePacket(packet)
            return
