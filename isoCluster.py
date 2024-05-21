import sys
import os
from pathlib import Path
import shutil
import ipaddress
from dataclasses import dataclass
import re
import time
import urllib.parse
from typing import Tuple
from logger import logger
from clustersConfig import ClustersConfig
from clustersConfig import NodeConfig
import host
import common


"""
ExtraConfigIPU is used to provision and IPUs specified via Redfish through the IMC.
This works by making some assumptions about the current state of the IPU:
- The IMC is on MeV 1.2 / Mev 1.3
- BMD_CONF has been set to allow for iso Boot
- ISCSI attempt has been added to allow for booting into the installed media
- The specified ISO contains full installation kickstart / kargs required for automated boot
- The specified ISO handles installing dependencies like dhclient and microshift
- The specified ISO architecture is aarch64
- There is an additional connection between the provisioning host and the acc on an isolated subnet to serve dhcp / provide acc with www
"""


@dataclass
class DhcpdSubnetConfig:
    subnet: str
    netmask: str
    range_start: str
    range_end: str
    broadcast_address: str
    routers: str
    dns_servers: list[str]


@dataclass
class DhcpdHostConfig:
    hostname: str
    hardware_ethernet: str
    fixed_address: str


def get_subnet_ip(ipv4_address: str, subnet_mask: str) -> str:
    subnet_mask_bits = subnet_mask.split('.')
    prefix_length = sum(bin(int(octet)).count('1') for octet in subnet_mask_bits)
    cidr_network = f"{ipv4_address}/{prefix_length}"
    network = ipaddress.ip_network(cidr_network, strict=False).network_address
    return str(network)


def get_subnet_range(ipv4_address: str, subnet_mask: str) -> Tuple[str, str]:
    subnet_mask_bits = subnet_mask.split('.')
    prefix_length = sum(bin(int(octet)).count('1') for octet in subnet_mask_bits)
    cidr_network = f"{ipv4_address}/{prefix_length}"
    network = ipaddress.ip_network(cidr_network, strict=False)
    range_start = network.network_address + 1
    range_end = network.broadcast_address - 1
    return str(range_start), str(range_end)


def get_router_ip(ipv4_address: str, subnet_mask: str) -> str:
    network = ipaddress.ip_network(f"{ipv4_address}/{subnet_mask}", strict=False)
    router_ip = network.network_address + 1
    return str(router_ip)


def subnetConfigFromHostConfig(hc: DhcpdHostConfig) -> DhcpdSubnetConfig:
    netmask = "255.255.255.0"
    subnet_ip = get_subnet_ip(hc.fixed_address, netmask)
    range_start, range_end = get_subnet_range(hc.fixed_address, netmask)
    broadcast_address = str(ipaddress.ip_network(f"{hc.fixed_address}/{netmask}", strict=False).broadcast_address)
    routers = get_router_ip(hc.fixed_address, netmask)
    dns_servers = ["10.2.70.215", "10.11.5.160"]
    return DhcpdSubnetConfig(subnet=subnet_ip, netmask=netmask, range_start=range_start, range_end=range_end, broadcast_address=broadcast_address, routers=routers, dns_servers=dns_servers)


def _convert_to_cidr(ipv4_address: str, subnet_mask: str) -> str:
    network = ipaddress.ip_network(f"{ipv4_address}/{subnet_mask}", strict=False)
    return str(network)


def extract_subnets_from_file(file_path: str) -> list[str]:
    subnet_pattern = re.compile(r'subnet (\d+\.\d+\.\d+\.\d+) netmask (\d+\.\d+\.\d+\.\d+)')
    with Path(file_path).open('r') as file:
        file_contents = file.read()

    subnets = []
    for subnet, netmask in subnet_pattern.findall(file_contents):
        subnets.append(_convert_to_cidr(subnet, netmask))

    return subnets


def extract_hostnames_from_file(file_path: str) -> list[str]:
    hostnames = []

    with open(file_path, 'r') as file:
        for line in file:
            if line.strip().startswith('host'):
                match = re.search(r'host\s+(\S+)', line.strip())
                if match:
                    hostnames.append(match.group(1))
    return hostnames


def render_dhcpd_conf(mac: str, ip: str, name: str) -> None:
    logger.debug("Rendering dhcpd conf")
    file_path = "/etc/dhcp/dhcpd.conf"
    hostconfig = DhcpdHostConfig(hostname=name, hardware_ethernet=mac, fixed_address=ip)
    subnetconfig = subnetConfigFromHostConfig(hostconfig)

    # If a config already exists, check if it was generated by CDA.
    file = Path(file_path)
    if file.exists():
        logger.debug("Existing dhcpd configuration detected")
        with file.open('r') as f:
            line = f.readline()
        # If not created by CDA, save as a backup to maintain idempotency
        if "Generated by CDA" not in line:
            logger.info("Backing up existing dhcpd conf to /etc/dhcp/dhcpd.conf.cda-backup")
            shutil.move(file_path, "/etc/dhcp/dhcpd.conf.cda-backup")
    file.touch()

    # Check if the current dhcp config already contains the host or subnet configuration, add a new entry if not
    if any(common.ip_in_subnet(hostconfig.fixed_address, subnet) for subnet in extract_subnets_from_file(file_path)):
        logger.debug(f"Subnet config for {hostconfig.fixed_address} already exists at {file_path}")
        subnet_config_str = ""
    else:
        logger.debug(f"Subnet config for {hostconfig.fixed_address} does not exist, creating this")
        subnet_config_str = f"""# Generated by CDA
subnet {subnetconfig.subnet} netmask {subnetconfig.netmask} {{
    range {subnetconfig.range_start} {subnetconfig.range_end};
    option domain-name-servers {", ".join(subnetconfig.dns_servers)};
    option routers {subnetconfig.routers};
    option broadcast-address {subnetconfig.broadcast_address};
}}
"""
    # TODO: improve this check to handle same hostname w/ different ip / mac, same ip / mac with new hostname etc. Very bad and buggy right now
    if hostconfig.hostname in extract_hostnames_from_file(file_path):
        logger.debug(f"Config for host {hostconfig.hostname} already exists")
        host_config_str = ""
    else:
        logger.debug(f"Config for host {hostconfig.hostname} does not exist, creating this")
        host_config_str = f"""# Generated by CDA
host {hostconfig.hostname} {{
    hardware ethernet {hostconfig.hardware_ethernet};
    fixed-address {hostconfig.fixed_address};
    option host-name {hostconfig.hostname};
}}
"""

    with file.open('a') as f:
        f.write(subnet_config_str)
        f.write(host_config_str)


def configure_dhcpd(node: NodeConfig) -> None:
    logger.info("Configuring dhcpd entry")

    render_dhcpd_conf(node.mac, str(node.ip), node.name)
    lh = host.LocalHost()
    ret = lh.run("systemctl restart dhcpd")
    if ret.returncode != 0:
        logger.error(f"Failed to restart dhcpd with err: {ret.err}")
        sys.exit(-1)


def configure_iso_network_port(api_port: str, node_ip: str) -> None:
    start, _ = get_subnet_range(node_ip, "255.255.255.0")
    lh = host.LocalHost()
    logger.info(f"Flushing cluster port {api_port} and setting ip to {start}")
    lh.run_or_die(f"ip addr flush dev {api_port}")
    lh.run_or_die(f"ip addr add {start}/24 dev {api_port}")


def enable_acc_connectivity(node: NodeConfig) -> None:
    logger.info(f"Establishing connectivity to {node.name} via {node.bmc}")
    ipu_imc = host.RemoteHost(node.bmc)
    ipu_imc.ssh_connect(node.bmc_user, node.bmc_password)
    ipu_imc.run_or_die("/usr/bin/scripts/cfg_acc_apf_x2.py")
    """
    We need to ensure the ACC physical port connectivity is enabled during reboot to ensure dhcp gets an ip.
    Trigger an acc reboot and try to run python /usr/bin/scripts/cfg_acc_apf_x2.py. This will fail until the
    ACC_LAN_APF_VPORTs are ready. Once this succeeds, we can try to connect to the ACC
    """
    logger.info("Rebooting IMC to trigger ACC reboot")
    ipu_imc.run("systemctl reboot")
    time.sleep(30)
    ipu_imc.ssh_connect(node.bmc_user, node.bmc_password)
    logger.info(f"Attempting to enable ACC connectivity from IMC {node.bmc} on reboot")
    retries = 30
    for _ in range(retries):
        ret = ipu_imc.run("/usr/bin/scripts/cfg_acc_apf_x2.py")
        if ret.returncode == 0:
            logger.info("Enabled ACC physical port connectivity")
            break
        logger.debug(f"ACC SPF script failed with returncode {ret.returncode}")
        logger.debug(f"out: {ret.out}\n err: {ret.err}")
        time.sleep(15)
    else:
        logger.error_and_exit("Failed to enable ACC connectivity")

    ipu_acc = host.RemoteHost(str(node.ip))
    ipu_acc.ping()
    ipu_acc.ssh_connect("root", "redhat")
    logger.info(f"{node.name} connectivity established")


def is_http_url(url: str) -> bool:
    try:
        result = urllib.parse.urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def _redfish_boot_ipu(cc: ClustersConfig, node: NodeConfig, iso: str) -> None:
    def helper(node: NodeConfig) -> str:
        logger.info(f"Booting {node.bmc} with {iso_address}")
        bmc = host.BMC.from_bmc(node.bmc)
        bmc.boot_iso_redfish(iso_path=iso_address, retries=5, retry_delay=15)

        """
        We need to determine that ACC has booted, however ACC will not have connectivity on reboot without manual
        intervention on the IMC. As a hack, wait until the installation is likely completed at which point we will try to
        establish ACC connectivity
        """
        imc = host.Host(node.bmc)
        imc.ssh_connect(node.bmc_user, node.bmc_password)
        time.sleep(25 * 60)
        return f"Finished booting imc {node.bmc}"

    # Ensure dhcpd is stopped before booting the IMC to avoid unintentionally setting the ACC hostname during the installation
    # https://issues.redhat.com/browse/RHEL-32696
    lh = host.LocalHost()
    lh.run("systemctl stop dhcpd")

    # If an http address is provided, we will boot from here.
    # Otherwise we will assume a local file has been provided and host it.
    if is_http_url(iso):
        logger.debug(f"Booting IPU from iso served at {iso}")
        iso_address = iso

        logger.info(helper(node))
    else:
        logger.debug(f"Booting IPU from local iso {iso}")
        if not os.path.exists(iso):
            logger.error(f"ISO file {iso} does not exist, exiting")
            sys.exit(-1)
        serve_path = os.path.dirname(iso)
        iso_name = os.path.basename(iso)
        lh = host.LocalHost()
        cc.prepare_external_port()
        lh_ip = common.port_to_ip(lh, cc.external_port)

        with common.HttpServerManager(serve_path, 8000) as http_server:
            iso_address = f"http://{lh_ip}:{str(http_server.port)}/{iso_name}"
            logger.info(helper(node))


def IPUIsoBoot(cc: ClustersConfig, node: NodeConfig, iso: str) -> None:
    _redfish_boot_ipu(cc, node, iso)
    assert node.ip is not None
    configure_iso_network_port(cc.network_api_port, node.ip)
    configure_dhcpd(node)
    enable_acc_connectivity(node)


def main() -> None:
    pass


if __name__ == "__main__":
    main()
