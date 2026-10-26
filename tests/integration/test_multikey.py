"""Phase 4, step 11: multi-key commands fan out across shards and gather the results."""


def test_mset_mget_across_shards(r):
    mapping = {f"user:{i}": f"name-{i}" for i in range(500)}
    assert r.mset(mapping) is True
    keys = list(mapping) + ["absent:1", "absent:2"]
    assert r.mget(keys) == [v.encode() for v in mapping.values()] + [None, None]


def test_del_and_exists_sum_across_shards(r):
    keys = [f"d{i}" for i in range(300)]
    r.mset({k: 1 for k in keys})
    assert r.exists(*keys) == 300
    assert r.delete(*keys[:100], "nope", keys[0]) == 100
    assert r.exists(*keys) == 200
    assert r.unlink(*keys) == 200
    assert r.dbsize() == 0


def test_hashtags_route_together(server, r):
    # All keys with the same {tag} live on one shard: a one-shard fan-out still works.
    keys = [f"{{cart:42}}:item:{i}" for i in range(50)]
    r.mset({k: i for i, k in enumerate(keys)})
    assert r.mget(keys) == [str(i).encode() for i in range(50)]
    assert r.delete(*keys) == 50


def test_mset_last_value_wins(r):
    assert r.execute_command("MSET", "k", "1", "k", "2", "k", "3") is True
    assert r.get("k") == b"3"


def test_dbsize_counts_every_shard(server, r):
    r.mset({f"z{i}": i for i in range(1000)})
    assert r.dbsize() == 1000
    info = server.info("keyspace")
    assert info["db0"]["keys"] == 1000
