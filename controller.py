"""Main interface to the duotecno bus."""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Final
from collections import deque
from yarl import URL
from .exceptions import LoadFailure, InvalidPassword
from .protocol import (
    NodeType,
    Packet,
    EV_CLIENTCONNECTSET_3,
    EV_NODEDATABASEINFO_0,
    EV_NODEDATABASEINFO_1,
    EV_HEARTBEATSTATUS_1,
)
from .node import Node
from .unit import BaseUnit, ControlUnit, DimUnit, DuoswitchUnit, SensUnit, SwitchUnit, VirtualUnit
import aiohttp


PW_TIMEOUT: Final = 5
CONNECT_TIMEOUT: Final = 10
JSON_TIMEOUT: Final = 10
LOAD_NODE_TIMEOUT: Final = 60
LOAD_UNIT_TIMEOUT: Final = 120
HB_TIMEOUT: Final = 20
HB_BUSEMPTY: Final = 60
RECONNECT_TIMEOUT: Final = 180


class PyDuotecno:
    """Class that will will do the bus management.

    - send packets
    - receive packets
    - open and close the connection
    """

    writer: asyncio.StreamWriter | None = None
    reader: asyncio.StreamReader | None = None
    readerTask: asyncio.Task[None]
    hbTask: asyncio.Task[None]
    workTask: asyncio.Task[None]
    writerTask: asyncio.Task[None]
    receiveQueue: asyncio.PriorityQueue
    sendQueue: asyncio.Queue[str]
    connectionOK: asyncio.Event
    heartbeatReceived: asyncio.Event
    nextHeartbeat: int
    packetToWaitFor: str | None = None
    packetWaiter: asyncio.Future[None] | None
    packetLock: asyncio.Lock
    nodes: dict[int, Node]
    host: str
    port: int
    password: str
    numNodes: int = 0
    json: dict[str, Any] | None = None
    json_url: str

    def __init__(self) -> None:
        self.nodes = {}

    def get_json(self) -> None | dict[str, Any]:
        if self.json is None:
            return None
        return self.json

    def get_units(self, unit_type: list[str] | str) -> list[BaseUnit]:
        res = []
        for node in self.nodes.values():
            for unit in node.get_unit_by_type(unit_type):
                res.append(unit)
        return res

    async def enableAllUnits(self) -> None:
        self._log.debug("Enable all Units on all nodes")
        for node in self.nodes.values():
            await node.enable()

    async def disableAllUnits(self) -> None:
        self._log.debug("Disable all Units on all nodes")
        for node in self.nodes.values():
            await node.disable()

    async def disconnect(self) -> None:
        self._log.debug("Disconnecting")
        connection_ok = getattr(self, "connectionOK", None)
        if connection_ok is not None:
            connection_ok.clear()

        for task_name in ("readerTask", "hbTask", "workTask", "writerTask"):
            task = getattr(self, task_name, None)
            if task is None:
                continue
            if task.cancel():
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        self._log.debug("Disconnecting Finished")

    async def connect(
        self, host: str, port: int, password: str, testOnly: bool = False, skipLoad: bool = False
    ) -> None:
        """Initialize the connection."""
        self.host = host
        self.port = port
        self.password = password
        self.json_url = self._build_json_url(host)
        await self._do_connect(testOnly=testOnly, skipLoad=skipLoad)

    def _build_json_url(self, host: str) -> str:
        return str(
            URL.build(scheme="http", host=host, port=8080).with_path(
                "/fs/download/config/nodedatabase.json"
            )
        )

    async def _reconnect(self):
        await self.disconnect()
        await self.disableAllUnits()
        await asyncio.sleep(RECONNECT_TIMEOUT)
        await self.continuously_check_connection()

    async def _do_connect(self, testOnly: bool = False, skipLoad: bool = False) -> None:
        if not skipLoad:
            self.nodes = {}
        self._log = logging.getLogger("pyduotecno")
        # Try to connect
        self._log.debug("Try to connect")
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=CONNECT_TIMEOUT,
            )
        except (ConnectionError, TimeoutError, asyncio.TimeoutError):
            raise
        # events
        self.connectionOK = asyncio.Event()
        self.heartbeatReceived = asyncio.Event()
        self.packetWaiter = None
        self.packetLock = asyncio.Lock()
        # at this point the connection should be ok
        self._log.debug("Connection established")
        self.connectionOK.set()
        self.heartbeatReceived.clear()
        self.nextHeartbeat = int(time.time()) + HB_BUSEMPTY
        # start the bus reading task
        self.receiveQueue = asyncio.PriorityQueue()
        self.sendQueue = asyncio.Queue()
        self.readerTask = asyncio.create_task(self._readTask())
        self.writerTask = asyncio.create_task(self._writeTask())
        self.workTask = asyncio.create_task(self._handleTask())
        # send login info
        passw = [str(ord(i)) for i in self.password]
        # wait for the login to be ok
        try:
            await asyncio.wait_for(
                self.waitForPacket(
                    "67,3,1",
                    f"[214,3,{len(passw)},{','.join(passw)}]",
                ),
                timeout=PW_TIMEOUT,
            )
        except TimeoutError:
            await self.disconnect()
            raise InvalidPassword()

        if testOnly:
            await self.disconnect()
            return

        self._log.info("Starting scan, json_url: %s", self.json_url)
        try:
            await self.scanWithJSON(url=self.json_url, skipLoad=skipLoad, testOnly=testOnly)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self._log.error(f"Error fetching JSON, Using bus scan client error: {e}")
            self.json = None
            await self.scanWithBus(skipLoad=skipLoad, testOnly=testOnly)
        except (ValueError, TypeError, KeyError) as e:
            self._log.error(f"Error parsing JSON scan data, Using bus scan: {e}")
            self.json = None
            await self.scanWithBus(skipLoad=skipLoad, testOnly=testOnly)

    async def fetch_json(self, url):
        timeout = aiohttp.ClientTimeout(total=JSON_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                response.raise_for_status()  # Raises if HTTP error
                return await response.json(content_type=None)

    def _map_json_node_type(self, node_type: str | int | NodeType) -> NodeType:
        if isinstance(node_type, NodeType):
            return node_type
        if isinstance(node_type, int):
            return NodeType(node_type)

        node_type_map = {
            "Std. Node": NodeType.Standard,
            "Standard": NodeType.Standard,
            "Gateway": NodeType.Gateway,
            "Modem": NodeType.Modem,
            "Gui": NodeType.Gui,
        }
        return node_type_map.get(node_type, NodeType.UNKNOWN)

    def _map_json_unit_type(self, unit_type: str) -> type[BaseUnit]:
        unit_type_map: dict[str, type[BaseUnit]] = {
            "Relais": SwitchUnit,
            "Sens": SensUnit,
            "Dimmer": DimUnit,
            "Motor": DuoswitchUnit,
            "Virtual": VirtualUnit,
            "Control": ControlUnit,
        }
        unit_class = unit_type_map.get(unit_type)
        if unit_class is None:
            self._log.warning(f"Unhandled unitType: {unit_type}")
            return BaseUnit
        return unit_class

    def _extract_json_devices(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, dict):
            if isinstance(data.get("devices"), list):
                return data["devices"]
            if isinstance(data.get("nodes"), list):
                return data["nodes"]
            if "nodedatabase" in data:
                return self._extract_json_devices(data["nodedatabase"])
            for value in data.values():
                if isinstance(value, (dict, list)):
                    try:
                        return self._extract_json_devices(value)
                    except KeyError:
                        continue
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                return data
        raise KeyError("devices")

    def _json_device_value(self, device: dict[str, Any], *keys: str, default: Any = None) -> Any:
        for key in keys:
            if key in device:
                return device[key]
        return default

    def _json_unit_value(self, unit: dict[str, Any], *keys: str, default: Any = None) -> Any:
        for key in keys:
            if key in unit:
                return unit[key]
        return default

    def _parse_json_int(self, value: Any, default: int = 0) -> int:
        if value is None:
            return default
        if isinstance(value, int):
            return value
        return int(str(value), 0)

    async def scanWithJSON(self, url: str, skipLoad: bool = False, testOnly: bool = False ) -> Any | None:
        self._log.info(f"Scanning with JSON: skipLoad: {skipLoad}, testOnly: {testOnly}")

        data = await self.fetch_json(url)
        # Different controller/legacy exports use slightly different top-level
        # shapes. Normalize them into the internal {"devices": [...]} format so
        # the rest of the integration can stay backend-agnostic.
        devices = self._extract_json_devices(data)
        normalized_devices: list[dict[str, Any]] = []
        for idx, device in enumerate(devices):
            units = self._json_device_value(device, "units", "device_units", default=[])
            node_type = self._map_json_node_type(
                self._json_device_value(
                    device, "type", "node_type", "nodeTypeName", default=NodeType.UNKNOWN
                )
            )
            address = self._parse_json_int(
                self._json_device_value(
                    device, "log_address", "address", "nodeAddress", default=0
                )
            )
            node_name = self._json_device_value(
                device, "name", "node_name", default=f"Node {address}"
            )
            physical_address = str(
                self._json_device_value(device, "physicalAddress", default="")
            ).strip() or None
            software_version = str(
                self._json_device_value(device, "softwareVersion", default="")
            ).strip() or None
            normalized_device = {
                "name": node_name,
                "log_address": hex(address),
                "physical_address": physical_address,
                "software_version": software_version,
                "nr_units": int(
                    self._json_device_value(
                        device,
                        "nr_units",
                        "num_units",
                        "numberOfUnits",
                        default=len(units),
                    )
                ),
                "type": self._json_device_value(
                    device, "type", "node_type", "nodeTypeName", default="Unknown"
                ),
                "units": [],
            }

            self.nodes[address] = Node(
                name=node_name,
                installation_id=f"{self.host}:{self.port}",
                address=address,
                index=idx,
                nodeType=node_type,
                numUnits=normalized_device["nr_units"],
                physicalAddress=physical_address,
                softwareVersion=software_version,
                writer=self.write,
                pwaiter=self.waitForPacket,
            )

            for idx2, unit in enumerate(units):
                # Keep the raw Duotecno name for stable entity naming. The JSON
                # friendlyName is preserved separately as metadata.
                unit_name = str(
                    self._json_unit_value(unit, "unit_name", "name", default="")
                )
                friendly_name = str(
                    self._json_unit_value(unit, "friendlyName", default="")
                ).strip()
                # The mobile apps hide units both via visibleInApps and via a
                # leading comma convention; mirror that filtering in HA.
                if not self._json_unit_value(unit, "visibleInApps", default=True):
                    continue
                if unit_name.startswith(","):
                    continue
                
                unit_type = str(
                    self._json_unit_value(
                        unit, "unit_type", "type", "unitTypeName", default=""
                    )
                )
                self._log.debug(f"Unit: {unit_type}")
                u = self._map_json_unit_type(unit_type)
                unit_address = self._parse_json_int(
                    self._json_unit_value(
                        unit, "unit_address", "address", "unit", "unitAddress", default=0
                    )
                )
                device_class = str(
                    self._json_unit_value(unit, "device_class", "deviceClass", default="")
                )
                self.nodes[address].units[unit_address] = u(
                    self.nodes[address],
                    name=unit_name,
                    friendly_name=friendly_name or None,
                    unit=unit_address,
                    device_class=device_class,
                    writer=self.nodes[address].writer,
                )
                normalized_device["units"].append(
                    {
                        "unit_name": unit_name,
                        "friendly_name": friendly_name,
                        "unit_address": hex(unit_address),
                        "unit_type": unit_type,
                        "device_class": device_class,
                        "area": str(self._json_unit_value(unit, "area", default="")),
                        "floor": str(self._json_unit_value(unit, "floor", default="")),
                        "building": str(self._json_unit_value(unit, "building", default="")),
                        "visible_in_apps": True,
                    }
                )
            self.nodes[address].isLoaded.set()
            normalized_devices.append(normalized_device)

        # Store the normalized export for the HA wrapper, which uses it for
        # area/floor assignment after entities are created.
        self.json = {"devices": normalized_devices}
        self._log.info("Requesting unit status")
        asyncio.create_task(self.requestStatus())
        self.hbTask = asyncio.create_task(self.heartbeatTask())

    async def requestStatus(self) -> None:
        for node in self.nodes.values():
            for unit in node.get_units():
                self._log.debug(f"Unit: {unit}")
                await unit.requestStatus()
                await asyncio.sleep(0.1)
        await self.enableAllUnits()

    async def scanWithBus(self, skipLoad: bool = False, testOnly: bool = False) -> None:
        if testOnly:
            return
        # do we need to reload the modules?
        if not skipLoad:
            await self.write("[209,5]")
            await self.write("[209,0]")
            try:
                await asyncio.wait_for(self._loadTaskNodes(), timeout=LOAD_NODE_TIMEOUT)
                self._log.info("Nodes discoverd")
                for n in self.nodes.values():
                    await n.load()
                    await asyncio.sleep(0.1)
                await asyncio.wait_for(self._loadTaskUnits(), timeout=LOAD_UNIT_TIMEOUT)
                self._log.info("Units discoverd")
            except TimeoutError:
                await self.disconnect()
                raise LoadFailure()
        # in case of skipload we do want to request the status again
        self._log.info("Requesting unit status")
        asyncio.create_task(self.requestStatus())
        self.hbTask = asyncio.create_task(self.heartbeatTask())

    async def write(self, msg: str) -> None:
        """Send a message."""
        if not self.writer:
            return
        if self.writer.transport.is_closing():
            await self._reconnect()
            return
        self._log.debug(f"TX: {msg}")
        await self.sendQueue.put(msg)
        return

    async def _writeTask(self) -> None:
        while True:
            try:
                msg = await self.sendQueue.get()
                msg = f"{msg}{chr(10)}"
                self.writer.write(msg.encode())
                await self.writer.drain()
                await asyncio.sleep(0.1)
            except ConnectionError:
                await self._reconnect()
                return

    async def _loadTaskNodes(self) -> None:
        while len(self.nodes) < 1:
            await asyncio.sleep(3)
        while True:
            if len(self.nodes) == self.numNodes:
                return
            await asyncio.sleep(1)

    async def _loadTaskUnits(self) -> None:
        while True:
            c = 0
            for n in self.nodes.values():
                if n.isLoaded.is_set():
                    c += 1
                if c == len(self.nodes):
                    return
            await asyncio.sleep(1)

    async def check_tcp_connection(self, timeout=3) -> bool:
        """Check if a TCP connection can be established to the given host and port."""
        self._log.debug("Checking connection...")
        conn = asyncio.open_connection(self.host, self.port)
        try:
            reader, writer = await asyncio.wait_for(conn, timeout=timeout)
            writer.close()
            await writer.wait_closed()
            return True
        except (asyncio.TimeoutError, OSError):
            # Could not connect within the timeout period or there was a network error
            return False

    async def continuously_check_connection(self) -> None:
        """Continuously check for connection restoration and reconnect."""
        self._log.info("Waiting for connection...")
        while True:
            connection_restored = await self.check_tcp_connection()
            if connection_restored:
                self._log.info("Connection to host restored, reconnecting.")
                await self._do_connect(skipLoad=True)
                break
            else:
                self._log.debug("Connection to host not yet restored, retrying...")
                await asyncio.sleep(5)

    async def heartbeatTask(self) -> None:
        await asyncio.sleep(30)
        self._log.info("Starting HB task")
        while True:
            # wait until the timer expire of 5 seconds
            while self.nextHeartbeat > int(time.time()):
                self._log.debug(
                    f"Waiting until {self.nextHeartbeat} to send a HB ({time.time()})"
                )
                await asyncio.sleep(5)
            # send the heartbeat
            self.heartbeatReceived.clear()
            try:
                self._log.debug("Sending heartbeat message")
                await self.write("[215,1]")
                await asyncio.wait_for(
                    self.heartbeatReceived.wait(), timeout=HB_TIMEOUT
                )
                self._log.debug("Received heartbeat message")
            except TimeoutError:
                self._log.warning("Timeout on heartbeat, reconnecting")
                await self._reconnect()
                break
            except asyncio.exceptions.CancelledError:
                break
        self._log.info("Stopping HB task")

    async def _readTask(self) -> None:
        """Reader task."""
        while self.connectionOK.is_set() and self.reader:
            try:
                tmp2 = await self.reader.readline()
            except ConnectionError:
                await self._reconnect()
                return
            if tmp2 == b"":
                return
            tmp3 = tmp2.decode()
            tmp = tmp3.rstrip().replace("\x00", "")
            if tmp.startswith("[") and tmp.endswith("]"):
                tmp = tmp[1:-1]
            elif tmp.startswith("["):
                tmp = tmp[1:]
            if tmp == "":
                return
            self._log.debug(f'RX: "{tmp}"')
            self.nextHeartbeat = int(time.time()) + HB_BUSEMPTY
            if await self._comparePacket(tmp):
                p = tmp.split(",")
                try:
                    pc = Packet(int(p[0]), int(p[1]), deque([int(_i) for _i in p[2:]]))
                    await self.receiveQueue.put(pc)
                # self._log.debug(f'QUEUE: "{pc}" {self.receiveQueue.qsize()}')
                except Exception:
                    self._log.exception("Failed to parse packet: %s", tmp)
            await asyncio.sleep(0.1)

    async def _comparePacket(self, rpck: str) -> bool:
        if not self.packetToWaitFor:
            return True
        received_parts = rpck.split(",")
        expected_parts = self.packetToWaitFor.split(",")
        if received_parts[: len(expected_parts)] == expected_parts:
            if self.packetWaiter is not None and not self.packetWaiter.done():
                self.packetWaiter.set_result(None)
            return True
        return False

    async def waitForPacket(self, pstr: str, msg: str | None = None) -> None:
        """Wait for a certain packet.

        Optionally send a message while holding the packet wait lock so fast
        replies cannot arrive before the waiter is installed.
        """
        async with self.packetLock:
            waiter = asyncio.get_running_loop().create_future()
            self.packetWaiter = waiter
            self.packetToWaitFor = pstr
            try:
                if msg is not None:
                    await self.write(msg)
                await waiter
            finally:
                if self.packetWaiter is waiter:
                    self.packetWaiter = None
                    self.packetToWaitFor = None

    async def _handleTask(self) -> None:
        """handler task."""
        while self.connectionOK.is_set() and self.receiveQueue:
            try:
                pc = await self.receiveQueue.get()
                self._log.debug(f"WX: {pc}")
                await self._handlePacket(pc)
            except Exception:
                self._log.exception("Unhandled packet handler failure")
            await asyncio.sleep(0.1)

    async def _handlePacket(self, packet: Packet) -> None:
        if packet.cls is None:
            self._log.debug(f"Ignoring packet: {packet}")
            return
        if isinstance(packet.cls, EV_CLIENTCONNECTSET_3):
            return
        if isinstance(packet.cls, EV_HEARTBEATSTATUS_1):
            self.heartbeatReceived.set()
            return
        if isinstance(packet.cls, EV_NODEDATABASEINFO_0):
            self.numNodes = packet.cls.numNode
            for i in range(packet.cls.numNode):
                await self.waitForPacket(f"64,1,{i}", f"[209,1,{i}]")
            return
        if isinstance(packet.cls, EV_NODEDATABASEINFO_1):
            if packet.cls.address not in self.nodes:
                self.nodes[packet.cls.address] = Node(
                    name=packet.cls.nodeName,
                    installation_id=f"{self.host}:{self.port}",
                    address=packet.cls.address,
                    index=packet.cls.index,
                    nodeType=packet.cls.nodeType,
                    numUnits=packet.cls.numUnits,
                    writer=self.write,
                    pwaiter=self.waitForPacket,
                )
                # await self.nodes[packet.cls.address].load()
            return
        if hasattr(packet.cls, "address") and packet.cls.address in self.nodes:
            await self.nodes[packet.cls.address].handlePacket(packet.cls)
            return
        self._log.debug(f"Ignoring packet: {packet}")
