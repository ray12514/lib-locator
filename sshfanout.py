import os
import subprocess
import time
from typing import List, Tuple

def run(cmd: List[str], timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
        timeout=timeout,
    )

def short_hostname(name: str) -> str:
    return name.split(".", 1)[0]

def classify_ssh_failure(rc: int, stderr: str) -> str:
    s = (stderr or "").lower()
    if rc == 0:
        return "ok"
    if rc == 255 and "permission denied" in s:
        return "policy_denied"
    if "could not resolve hostname" in s or "name or service not known" in s or "temporary failure in name resolution" in s:
        return "dns"
    if "host key verification failed" in s:
        return "hostkey"
    if "remote host identification has changed" in s:
        return "hostkey_changed"
    if "connection timed out" in s or "operation timed out" in s:
        return "timeout"
    if "no route to host" in s:
        return "no_route"
    if "connection refused" in s:
        return "refused"
    if "kex_exchange_identification" in s or "connection reset by peer" in s or "connection closed by remote host" in s:
        return "reset_or_throttle"
    if rc == 255:
        return "ssh_error"
    return "remote_exec_error"

class SSHConfig:
    def __init__(
        self,
        known_hosts: str,
        hostkey_mode: str,
        loglevel: str,
        connect_timeout: int,
        server_alive_interval: int,
        server_alive_count_max: int,
        control_master: bool,
        control_persist: str,
        control_path: str,
    ) -> None:
        self.known_hosts = known_hosts
        self.hostkey_mode = hostkey_mode
        self.loglevel = loglevel
        self.connect_timeout = connect_timeout
        self.server_alive_interval = server_alive_interval
        self.server_alive_count_max = server_alive_count_max
        self.control_master = control_master
        self.control_persist = control_persist
        self.control_path = control_path

    def base_args(self) -> List[str]:
        args = [
            "ssh",
            "-T",
            "-n",
            "-o", "BatchMode=yes",
            "-o", "StdinNull=yes",
            "-o", f"StrictHostKeyChecking={self.hostkey_mode}",
            "-o", f"UserKnownHostsFile={self.known_hosts}",
            "-o", "GlobalKnownHostsFile=/dev/null",
            "-o", f"LogLevel={self.loglevel}",
            "-o", f"ConnectTimeout={self.connect_timeout}",
            "-o", f"ServerAliveInterval={self.server_alive_interval}",
            "-o", f"ServerAliveCountMax={self.server_alive_count_max}",
        ]
        if self.control_master:
            args += [
                "-o", "ControlMaster=auto",
                "-o", f"ControlPersist={self.control_persist}",
                "-o", f"ControlPath={self.control_path}",
            ]
        return args

def default_ssh_config() -> SSHConfig:
    cache_dir = os.path.expanduser("~/.cache/lib_sweep")
    os.makedirs(cache_dir, exist_ok=True)
    known_hosts = os.path.join(cache_dir, "known_hosts")
    control_path = os.path.join(cache_dir, "ctl_%r@%h:%p")
    return SSHConfig(
        known_hosts=known_hosts,
        hostkey_mode="accept-new",
        loglevel="ERROR",
        connect_timeout=5,
        server_alive_interval=5,
        server_alive_count_max=1,
        control_master=False,
        control_persist="60s",
        control_path=control_path,
    )

def ssh(node: str, argv: List[str], cfg: SSHConfig, timeout: int) -> subprocess.CompletedProcess:
    node = short_hostname(node)
    cmd = cfg.base_args() + [node] + argv
    return run(cmd, timeout=timeout)

def ssh_with_retries(node: str, argv: List[str], cfg: SSHConfig, timeout: int, retries: int) -> Tuple[subprocess.CompletedProcess, str]:
    last = None
    last_kind = "ssh_error"
    for attempt in range(retries + 1):
        try:
            p = ssh(node, argv, cfg, timeout=timeout)
        except subprocess.TimeoutExpired as ex:
            tout_stdout = ex.stdout.decode(errors="replace") if isinstance(ex.stdout, bytes) else (ex.stdout or "")
            tout_stderr = ex.stderr.decode(errors="replace") if isinstance(ex.stderr, bytes) else (ex.stderr or "")
            p = subprocess.CompletedProcess(
                args=ex.cmd,
                returncode=255,
                stdout=tout_stdout,
                stderr=tout_stderr + "\noperation timed out",
            )
        last = p
        last_kind = classify_ssh_failure(p.returncode, p.stderr)
        if p.returncode == 0:
            break
        if attempt < retries and (p.returncode == 255 or last_kind in ("reset_or_throttle", "timeout")):
            time.sleep(0.5 * (attempt + 1))
        else:
            break
    assert last is not None
    return last, last_kind
