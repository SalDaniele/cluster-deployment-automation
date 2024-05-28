from clustersConfig import ClustersConfig, NodeConfig
import host
from k8sClient import K8sClient
from concurrent.futures import Future, ThreadPoolExecutor
import jinja2
import re
import os
import requests
import time
from typing import Optional, Match
from logger import logger
from clustersConfig import ExtraConfigArgs
import reglocal
from common import git_repo_setup

DPU_OPERATOR_REPO = "https://github.com/openshift/dpu-operator.git"
IPU_OPI_PLUGIN_REPO = "https://github.com/intel/ipu-opi-plugins.git"
MICROSHIFT_KUBECONFIG = "/var/lib/microshift/resources/kubeadmin/kubeconfig"
OSE_DOCKERFILE = "https://pkgs.devel.redhat.com/cgit/containers/dpu-operator/tree/Dockerfile?h=rhaos-4.17-rhel-9"
REPO_DIR = "/root/dpu-operator"
SRIOV_NUM_VFS = 8


def _update_dockerfile(image: str, path: str) -> None:
    with open(path, 'r') as file:
        dockerfile_contents = file.read()

    # Update only the non-builder image
    pattern = re.compile(r'^FROM\s+([^\s]+)(?!.*\bAS\b.*$)', re.MULTILINE)

    def replace_image(match: Match[str]) -> str:
        return f"FROM {image}"

    new_dockerfile_contents = pattern.sub(replace_image, dockerfile_contents)

    with open(path, 'w') as file:
        file.write(new_dockerfile_contents)


def _get_ose_image(dockerfile_url: str) -> str:
    logger.info("Fetching")
    request = requests.get(dockerfile_url, verify=False)
    image = None
    for line in request.text.split("\n"):
        if line.startswith("FROM"):
            image = line.split(" ")[1]
    if image:
        src = "openshift/"
        dst = "registry-proxy.engineering.redhat.com/rh-osbs/openshift-"
        return image.replace(src, dst)
    else:
        logger.error_and_exit(f"Failed to parse base image from {dockerfile_url}")


def update_dockerfiles_with_ose_images(repo: str, dockerfile_url: str = OSE_DOCKERFILE) -> None:
    image = _get_ose_image(dockerfile_url)
    for file in ["/root/dpu-operator/Dockerfile.rhel", "/root/dpu-operator/Dockerfile.daemon.rhel"]:
        _update_dockerfile(image, file)


def _ensure_local_registry_running(rsh: host.Host, delete_all: bool = False) -> str:
    logger.info(f"creating local registry on {rsh.hostname()}")
    _, reglocal_hostname, reglocal_listen_port, _ = reglocal.ensure_running(rsh, delete_all=delete_all)
    registry = f"{reglocal_hostname}:{reglocal_listen_port}"
    return registry


def extractContainerImage(dockerfile: str) -> str:
    match = re.search(r'FROM\s+([^\s]+)(?:\s+as\s+\w+)?', dockerfile, re.IGNORECASE)
    if match:
        first_image = match.group(1)
        return first_image
    else:
        logger.error_and_exit("Failed to find a Docker image in provided output")


def ensure_go_installed(host: host.Host) -> None:
    if host.run("go verison").returncode != 0:
        host.run_or_die("wget https://go.dev/dl/go1.22.3.linux-arm64.tar.gz")
        host.run_or_die("tar -C /usr/local -xzf go1.22.3.linux-arm64.tar.gz")
        host.run_or_die("echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile")
        host.run_or_die("echo 'export PATH=$PATH:/usr/local/go/bin' > /etc/profile.d/go.sh")
        host.run_or_die("chmod +x /etc/profile.d/go.sh")


def copy_local_registry_certs(host: host.Host, path: str) -> None:
    directory = "/root/.local-container-registry/certs"
    files = os.listdir(directory)
    for file in files:
        host.copy_to(f"{directory}/{file}", f"{path}/{file}")


def build_dpu_operator_images() -> str:
    logger.info("Building dpu operator images")
    lh = host.LocalHost()
    git_repo_setup(REPO_DIR, repo_wipe=True, url=DPU_OPERATOR_REPO, branch="main")
    update_dockerfiles_with_ose_images(REPO_DIR)

    # Start a local registry to store dpu-operator images
    registry = _ensure_local_registry_running(lh, delete_all=True)
    reglocal.local_trust(lh)

    operator_image = f"{registry}/openshift-dpu-operator/cda-dpu-operator:latest"
    daemon_image = f"{registry}/openshift-dpu-operator/cda-dpu-daemon:latest"
    render_local_images_yaml(operator_image=operator_image, daemon_image=daemon_image, outfilename="/root/dpu-operator/config/dev/local-images.yaml")

    lh.run_or_die("make -C /root/dpu-operator images-buildx")

    return registry


def build_and_start_vsp(host: host.Host, client: K8sClient, registry: str) -> None:
    logger.info("Building ipu-opi-plugin")
    host.run("rm -rf /root/ipu-opi-plugins")
    host.run_or_die(f"git clone {IPU_OPI_PLUGIN_REPO}")
    ret = host.run_or_die("cat /root/ipu-opi-plugins/ipu-plugin/images/Dockerfile")
    golang_img = extractContainerImage(ret.out)
    host.run_or_die(f"podman pull docker.io/library/{golang_img}")
    host.run_or_die("cd /root/ipu-opi-plugins/ipu-plugin && export IMGTOOL=podman && make image")
    vsp_image = f"{registry}/ipu-plugin:dpu"
    host.run_or_die(f"podman tag intel-ipuplugin:latest {vsp_image}")

    render_dpu_vsp_ds(vsp_image, "/tmp/vsp-ds.yaml")
    host.copy_to("/tmp/vsp-ds.yaml", "/tmp/vsp-ds.yaml")
    client.oc("delete -f /tmp/vsp-ds.yaml")
    client.oc_run_or_die("create -f /tmp/vsp-ds.yaml")


def start_dpu_operator(host: host.Host, client: K8sClient, operator_image: str, daemon_image: str, repo_wipe: bool = False) -> None:
    logger.info(f"Deploying dpu operator containers on {host.hostname()}")
    if repo_wipe:
        host.run("rm -rf /root/dpu-operator")
        host.run_or_die(f"git clone {DPU_OPERATOR_REPO}")
        render_local_images_yaml(operator_image=operator_image, daemon_image=daemon_image, outfilename="/tmp/dpu-local-images.yaml", pull_policy="IfNotPresent")
        host.copy_to("/tmp/dpu-local-images.yaml", "/root/dpu-operator/config/dev/local-images.yaml")

    host.run_or_die("pip install yq")
    ensure_go_installed(host)
    reglocal.local_trust(host)
    host.run_or_die(f"podman pull {operator_image}")
    host.run_or_die(f"podman pull {daemon_image}")
    host.run(f"cd /root/dpu-operator && export KUBECONFIG={client._kc} && make undeploy")
    host.run_or_die(f"cd /root/dpu-operator && export KUBECONFIG={client._kc} && make local-deploy")
    logger.info("Waiting for all pods to become ready")


def render_local_images_yaml(operator_image: str, daemon_image: str, outfilename: str, pull_policy: str = "Always") -> None:
    with open('./manifests/dpu/local-images.yaml.j2') as f:
        j2_template = jinja2.Template(f.read())
        rendered = j2_template.render(operator_image=operator_image, daemon_image=daemon_image, pull_policy=pull_policy)
        logger.info(rendered)

    with open(outfilename, "w") as outFile:
        outFile.write(rendered)


def render_dpu_vsp_ds(ipu_plugin_image: str, outfilename: str) -> None:
    with open('./manifests/dpu/dpu_vsp_ds.yaml.j2') as f:
        j2_template = jinja2.Template(f.read())
        rendered = j2_template.render(ipu_plugin_image=ipu_plugin_image)
        logger.info(rendered)

    with open(outfilename, "w") as outFile:
        outFile.write(rendered)


def ExtraConfigDpu(cc: ClustersConfig, cfg: ExtraConfigArgs, futures: dict[str, Future[Optional[host.Result]]]) -> None:
    [f.result() for (_, f) in futures.items()]
    logger.info("Running post config step to start DPU operator on IPU")

    ipu_node = cc.masters[0]
    assert ipu_node.ip is not None
    acc = host.Host(ipu_node.ip)
    lh = host.LocalHost()
    acc.ssh_connect("root", "redhat")
    client = K8sClient(MICROSHIFT_KUBECONFIG, acc)

    if cfg.rebuild_dpu_operators_images:
        registry = build_dpu_operator_images()
    else:
        registry = _ensure_local_registry_running(lh, delete_all=False)

    operator_image = f"{registry}/openshift-dpu-operator/cda-dpu-operator:latest"
    daemon_image = f"{registry}/openshift-dpu-operator/cda-dpu-daemon:latest"

    # TODO: Remove when this container is properly started by the vsp
    # We need to manually start the p4 sdk container currently
    img = "quay.io/sdaniele/intel-ipu-p4-sdk:temp_wa_5-28-24"
    cmd = f"podman run --network host -d --privileged --entrypoint='[\"/bin/sh\", \"-c\", \"sleep 5; sh /entrypoint.sh\"]' -v /lib/modules/5.14.0-425.el9.aarch64:/lib/modules/5.14.0-425.el9.aarch64 -v data1:/opt/p4 {img}"
    logger.info("Manually starting P4 container")
    acc.run_or_die(cmd)

    # Build and start vsp on DPU
    build_and_start_vsp(acc, client, registry)

    start_dpu_operator(acc, client, operator_image, daemon_image, repo_wipe=True)

    # Deploy dpu daemon
    client.oc_run_or_die(f"label no {ipu_node.name} dpu=true")
    logger.info("Waiting for all pods to become ready")
    client.oc_run_or_die("wait --for=condition=Ready pod --all --all-namespaces --timeout=1m")
    client.oc_run_or_die("create -f /root/dpu-operator/examples/dpu.yaml")
    time.sleep(30)

    # TODO: remove wa once fixed in future versions of MeV
    # Wait for dpu to restart after vsp triggers reboot
    # Note, this will fail if the acc comes up with a new MAC address on the physical port.
    # As a temporary workaround until this issue is resolved, pre-load the rh_mvp.pkg / configure the iscsi attempt
    # to ensure the MAC remains consistent across reboots
    acc.ssh_connect("root", "redhat")
    cmd = f"podman run --network host -d --privileged --entrypoint='[\"/bin/sh\", \"-c\", \"sleep 5; sh /entrypoint.sh\"]' -v /lib/modules/5.14.0-425.el9.aarch64:/lib/modules/5.14.0-425.el9.aarch64 -v data1:/opt/p4 {img}"
    logger.info("Manually restarting P4 container")
    acc.run_or_die(cmd)
    acc.run_or_die("systemctl restart microshift")
    client.oc_run_or_die("wait --for=condition=Ready pod --all --all-namespaces --timeout=2m")


def ExtraConfigDpuHost(cc: ClustersConfig, cfg: ExtraConfigArgs, futures: dict[str, Future[Optional[host.Result]]]) -> None:
    logger.info("Running post config step to start DPU operator on Host")

    lh = host.LocalHost()
    client = K8sClient(cc.kubeconfig)

    if cfg.rebuild_dpu_operators_images:
        registry = build_dpu_operator_images()
    else:
        registry = _ensure_local_registry_running(lh, delete_all=False)
    operator_image = f"{registry}/openshift-dpu-operator/cda-dpu-operator:latest"
    daemon_image = f"{registry}/openshift-dpu-operator/cda-dpu-daemon:latest"

    # Need to trust the registry in OCP / Microshift
    reglocal.ocp_trust(client, reglocal._dir_name(lh), reglocal._hostname(lh), 5000)

    build_and_start_vsp(lh, client, registry)

    start_dpu_operator(lh, client, operator_image, daemon_image)
    client.oc_run_or_die("wait --for=condition=Ready pod --all -n dpu-operator-system --timeout=1m")

    def helper(h: host.Host, node: NodeConfig) -> Optional[host.Result]:
        # There is a bug with the idpf driver that causes the IPU to fail to be enumerated over PCIe on boot
        # As a result, we will need to trigger cold boots of the node until the device is available
        # TODO: Remove when no longer needed
        retries = 10
        h.ssh_connect("core")
        while h.run(f"test -d /sys/class/net/{cfg.dpu_net_interface}").returncode != 0:
            logger.error(f"{h.hostname()} does not have a network device {cfg.dpu_net_interface} cold booting node to try to recover")
            h.cold_boot()
            h.ssh_connect("core")
            retries -= 1
            if retries == 0:
                logger.error_and_exit(f"Failed to bring up IPU net device on {h.hostname()}")

        # Temporarily we will need to manually create the vfs on each DPU host
        # TODO: Remove when no longer required
        h.run_or_die(f"echo {SRIOV_NUM_VFS} > /sys/class/net/{cfg.dpu_net_interface}/device/sriov_numvfs")

        # Label the node
        client.oc_run_or_die(f"label no {e.name} dpu=true")
        return None

    executor = ThreadPoolExecutor(max_workers=len(cc.workers))
    # Assuming that all workers have a DPU
    for e in cc.workers:
        h = host.Host(e.node)
        futures[e.name].result()
        f = executor.submit(helper, h, e)
        futures[e.name] = f

    # Create host nad
    # TODO: Remove when this is automatically created by the dpu operator
    client.oc_run_or_die("create -f /manifests/dpu/dpu_nad.yaml")
    # Deploy dpu daemon and wait for dpu pods to come up
    client.oc_run_or_die("create -f /root/dpu-operator/examples/dpu.yaml")
    client.oc_run_or_die("wait --for=condition=Ready pod --all -n dpu-operator-system --timeout=1m")
    time.sleep(30)


def main() -> None:
    pass


if __name__ == "__main__":
    main()
