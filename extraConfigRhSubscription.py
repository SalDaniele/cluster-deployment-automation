from concurrent.futures import Future, ThreadPoolExecutor
from typing import Optional
import sys
from tenacity import retry, stop_after_attempt, wait_incrementing
from logger import logger
from clustersConfig import ClustersConfig, NodeConfig
from clustersConfig import ExtraConfigArgs
import host
from extraConfigMicroshift import masquarade


def ExtraConfigRhSubscription(cc: ClustersConfig, cfg: ExtraConfigArgs, futures: dict[str, Future[Optional[host.Result]]]) -> None:
    [f.result() for (_, f) in futures.items()]

    cc.prepare_external_port()
    lh = host.LocalHost()
    masquarade(lh, cc)

    logger.info("Running post config step to attach Red Hat subscription")

    if cfg.organization_id is None:
        logger.error_and_exit("Error no organization id was specified to attach Red Hat subscription")

    if cfg.activation_key is None:
        logger.error_and_exit("Error no activation key was specified to attach Red Hat subscription")

    @retry(stop=stop_after_attempt(3), wait=wait_incrementing(start=1, increment=30))
    def helper(node: NodeConfig) -> host.Result:
        logger.info(f"attaching subscription on {node.name}")
        assert node.ip is not None
        h = host.Host(node.ip)
        h.ssh_connect("root", "redhat")
        ret = h.run(f"rhc connect -o {cfg.organization_id} -a {cfg.activation_key}", quiet=True)
        if not ret.success():
            logger.error(f"Retrying attachment on {node.name} due to failure: {ret.err}")
            raise Exception(f"Failed to attach subscription on {node.name}")
        return ret

    executor = ThreadPoolExecutor(max_workers=len(cc.all_nodes()))
    # Assume we are attaching subscription on all nodes

    f = []
    for node in cc.all_nodes():
        f.append(executor.submit(helper, node))

    for thread in f:
        ret = thread.result()
        logger.info(ret.out)
        if ret.returncode != 0:
            logger.error(f"Failed to attach subscription: {ret.err}")
            sys.exit(-1)
