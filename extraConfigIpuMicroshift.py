import time
import itertools
from concurrent.futures import Future
from typing import Optional
from logger import logger
from clustersConfig import ClustersConfig
from clustersConfig import ExtraConfigArgs
import host


def ExtraConfigIpuMicroshift(cc: ClustersConfig, cfg: ExtraConfigArgs, futures: dict[str, Future[Optional[host.Result]]]) -> None:
    [f.result() for (_, f) in futures.items()]
    logger.info("Running post config step to start Microshift on the IPU")

    # Validate args
    ipu_node = cc.masters[0]

    # Enable NAT on host to provide internet connectivity to ACC
    lh = host.LocalHost()
    cc.prepare_external_port()
    wan_interface = cc.external_port
    lan_interface = cc.network_api_port
    ip_tables = "/sbin/iptables"

    logger.info(f"Setting up ip forwarding on {lh.hostname()} from {lan_interface} to {wan_interface}")

    lh.run_or_die("/bin/echo 1 > /proc/sys/net/ipv4/ip_forward")
    lh.run_or_die(f"{ip_tables} -t nat -A POSTROUTING -o {lan_interface} -j MASQUERADE")
    lh.run_or_die(f"{ip_tables} -A FORWARD -i {lan_interface} -o {wan_interface} -m state --state RELATED,ESTABLISHED -j ACCEPT ")
    lh.run_or_die(f"{ip_tables} -A FORWARD -i {wan_interface} -o {lan_interface} -j ACCEPT")
    lh.run_or_die(f"{ip_tables} -t nat -A POSTROUTING -o {wan_interface} -j MASQUERADE")
    lh.run_or_die(f"{ip_tables} -A FORWARD -i {wan_interface} -o {lan_interface} -m state --state RELATED,ESTABLISHED -j ACCEPT")
    lh.run_or_die(f"{ip_tables} -A FORWARD -i {lan_interface} -o {wan_interface} -j ACCEPT")

    # set up internet connectivity on ACC
    assert ipu_node.ip is not None
    acc = host.Host(ipu_node.ip)
    # copy dns resolution from provisioning host
    logger.info(f"Enabling networking via enp0s1f0d1 on {acc.hostname()}")
    acc.ssh_connect("root", "redhat")
    acc.copy_to("/etc/resolv.conf", "/etc/resolv.conf")
    # Delete default route
    acc.run_or_die("ip route del default via 192.168.0.1 dev enp0s1f0")
    # Verify we have connectivity to www
    ret = acc.run("wget google.com")
    if ret.returncode != 0:
        logger.error_and_exit("Failed to establish ACC internet connectivity")
    else:
        logger.info("ACC Internet connectivity established successfully")

    # Manually set datetime on ACC
    host.sync_time(lh, acc)

    # We already should have a hostname from the dhcp lease, but for some reason we need to set this again for microshift 4.15 to come up
    lh.run_or_die(f"hostnamectl set-hostname {ipu_node.name}")

    # Start microshift?
    acc.run("systemctl start microshift.service")  # this fails...will it always? Is it needed?

    # Set up pull secret
    logger.info(f"Copying pull secret to {acc.hostname}:/etc/crio/openshift-pull-secret")
    acc.copy_to("pull_secret.json", "/etc/crio/openshift-pull-secret")
    acc.run_or_die("chown root:root /etc/crio/openshift-pull-secret")
    acc.run_or_die("chmod 600 /etc/crio/openshift-pull-secret")

    # Configure firewalld for microshift
    logger.info(f"Configuring firewall for microshift")
    acc.run_or_die("firewall-cmd --permanent --zone=trusted --add-source=10.42.0.0/16")
    acc.run_or_die("firewall-cmd --permanent --zone=trusted --add-source=169.254.169.1")

    # Restart microshift
    acc.run("systemctl restart microshift")

    # Given things time to settle
    time.sleep(180)

    # Check on the status of the cluster
    kubeconfig = "/var/lib/microshift/resources/kubeadmin/kubeconfig"
    ret = acc.run(f"oc get no --kubeconfig {kubeconfig}")

    if ret.returncode == 0:
        logger.info("Microshift started successfully, verifying nodes are up")
    else:
        logger.error(f"Failed to connect to microshift service initially with err {ret.returncode}: {ret.err}, will continue trying")

    for attempt in itertools.repeat(None, 100):
        ret = acc.run(f"oc get no --kubeconfig {kubeconfig}")
        if ret.returncode == 0:
            if "Ready" in ret.out:
                logger.info("Verified microshift node is ready")
                break
        else:
            logger.info(f"Microshift endpoint is not yet available, retrying, {ret.err}")
        logger.info(f"Microshift node not yet ready, attept {attempt}")
        time.sleep(60)
    else:
        logger.error_and_exit(f"Node failed to reach ready state {ret.returncode}: {ret.out} {ret.err}")
