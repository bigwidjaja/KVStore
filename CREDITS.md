# Credits

This project's architecture is modeled on [Dragonfly](https://github.com/dragonflydb/dragonfly) by DragonflyDB and its I/O library [helio](https://github.com/romange/helio) by Roman Gershman. Design choices taken from them are tagged DC-01 to DC-20 in `docs/DESIGN.md`, with the source file each came from. No Dragonfly or helio code is included.

Sources were read at Dragonfly commit `d8cd94b` and helio commit `40b7b20` (Oct 23, 2026). In the code, each borrowed choice is marked where it is implemented with a comment of the form `Design after Dragonfly (DC-NN): <source file>`.

## License notes

- **Dragonfly** is under the [Business Source License 1.1](https://github.com/dragonflydb/dragonfly/blob/main/LICENSE.md). We copy none of its code; ideas and designs are reimplemented here from scratch, with credit.
- **helio** is under [Apache 2.0](https://github.com/romange/helio/blob/master/LICENSE). If a helio snippet is ever adapted, its license header stays and a `NOTICE` entry is added. None is today.

## Other references

- [Dash: Scalable Hashing on Persistent Memory](https://arxiv.org/abs/2003.07302), the paper Dashtable builds on (DC-16).
- Dmitry Vyukov, [Bounded MPMC queue](http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue), the queue helio uses (DC-05).
- [VLL: a lock manager redesign for main memory database systems](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf) (DC-20, not yet implemented).
- [RESP protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/) (DC-08). Error texts and argument rules follow Redis 7.
