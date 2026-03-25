"""Network-related helpers shared across Crucible components."""


def parse_host(host_str: str, port: int | str | None = None) -> tuple[str, int]:
    """Resolve a host/port pair from a host string and an optional explicit port.

    Supports two conventions found in Crucible test plans::

        # Port embedded in the host string
        cluster_info:
          host: "doris-fe:9030"

        # Port as a separate field
        cluster_info:
          host: "doris-fe.svc.cluster.local"
          port: 9030

    When *port* is provided it takes precedence over any port embedded in
    *host_str*.  If neither source supplies a port, a ``ValueError`` is raised.
    """
    if port is not None:
        return host_str, int(port)
    if ":" not in host_str:
        raise ValueError(
            f"host must include a port (e.g. 'hostname:9030'), got: {host_str!r}"
        )
    host, _, port_str = host_str.rpartition(":")
    return host, int(port_str)
