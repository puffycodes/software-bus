# Bus Check Design

## Purpose

Nodes must be connected as a tree. Publish and Subscribe Nodes refuse a connection that would close a cycle (see `publish-subscribe.md`), but Base Layer Nodes do not, and a cycle of Base Layer Nodes makes every message circulate forever.

`bus_check` finds cycles among the nodes running on this machine by looking at their TCP connections, as the operating system reports them. It sends nothing over the bus and needs no change to the wire format, so it works for both kinds of node.

## bus_check Script

- Find the nodes running on this machine.
    - A node is a process running `bl_server` or `ps_server`, whether started as `python -m software_bus.bl_server` (or `ps_server`) or as the installed `bl_server` / `ps_server` command.
    - parameter --pid PID (may be repeated; default: None) also treats the process PID as a node, e.g. a program that uses a Base Layer Node or a Publish and Subscribe Node itself.
- Find the links between those nodes: every established TCP connection whose two ends both belong to nodes on this machine.
    - Connections from a node to a process that is not a node (e.g. a client) are ignored: a process with only one connection cannot be part of a cycle.
    - Connections from a node to another machine cannot be checked. Report how many there are, as a warning.
    - If the operating system hides some connections' owning processes (this can need administrator or root rights), report it as a warning: the check may be incomplete.
- Report every cycle among the nodes and links found.
    - Treat the links as an undirected graph with possibly several links between two nodes (the base layer floods in both directions over every connection).
    - Each link that joins two nodes already connected through other links closes one cycle; report that cycle as the list of nodes along it.
    - This includes two links between the same two nodes (a cycle of two), and a node connected to itself.
    - Show each node by process ID, kind (`bl_server`, `ps_server`, or `node` for one given with --pid) and the addresses it listens on.
- Exit status:
    - 0 if no cycle is found.
    - 1 if at least one cycle is found.
    - 2, with an error message, if the check cannot be run (e.g. `psutil` is not installed, or the operating system refuses to list connections).
- Print logging information
    - parameter --debug true|false (default: false)

## Dependency

Listing processes and their connections uses `psutil`. It is an optional dependency (the `check` extra: `pip install "software-bus[check]"`), so the rest of the package does not need it.

## Limitations

- Only one machine is checked. Nodes connected across machines need the check run on each machine, and a cycle that passes through several machines is not found.
- Only connections that exist at the moment of the check are seen.
