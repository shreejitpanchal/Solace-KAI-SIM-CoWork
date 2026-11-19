import pytest

from kai_sim_cowork import topics
from kai_sim_cowork.topics import TopicSpace


def test_event_and_command_topics_follow_the_brief(space):
    assert (
        space.event_topic("orchestration", "task", "created")
        == "cowork/dev/apse1/orchestration/task/created/v1"
    )
    assert space.command_topic("agent", "execute") == "cowork/dev/apse1/agent/command/execute/v1"


def test_topic_for_maps_event_type_to_topic(space):
    assert (
        space.topic_for("cowork.orchestration.task.created")
        == "cowork/dev/apse1/orchestration/task/created/v1"
    )
    assert space.topic_for("cowork.tooling.command.execute") == (
        "cowork/dev/apse1/tooling/command/execute/v1"
    )


@pytest.mark.parametrize("bad", ["orchestration.task.created", "cowork.task.created", "x.a.b.c.d"])
def test_topic_for_rejects_malformed_event_types(space, bad):
    with pytest.raises(ValueError, match="eventType"):
        space.topic_for(bad)


def test_subscriptions_use_wildcards_only_where_asked(space):
    assert space.event_subscription("orchestration", "task") == (
        "cowork/dev/apse1/orchestration/task/*/v1"
    )
    assert space.event_subscription("agent") == "cowork/dev/apse1/agent/*/*/v1"
    assert space.event_subscription("tooling", "tool", "progressed") == (
        "cowork/dev/apse1/tooling/tool/progressed/v1"
    )
    assert space.command_subscription("agent", "execute") == (
        "cowork/dev/apse1/agent/command/execute/v1"
    )
    assert space.command_subscription("tooling") == "cowork/dev/apse1/tooling/command/*/v1"
    assert space.all_subscription() == "cowork/dev/apse1/>"


@pytest.mark.parametrize("bad", ["", "a/b", ">", "x" * 65, "sp ace", None, 42])
def test_validate_level_rejects_unsafe_values(bad):
    with pytest.raises(ValueError, match="domain"):
        topics.validate_level(bad, "domain")


@pytest.mark.parametrize("bad", ["a/b", ">", "task/*"])
def test_builders_never_emit_unvalidated_levels(space, bad):
    with pytest.raises(ValueError):
        space.event_topic(bad, "task", "created")
    with pytest.raises(ValueError):
        space.command_topic("agent", bad)
    with pytest.raises(ValueError):
        space.event_subscription("orchestration", bad)
    with pytest.raises(ValueError):
        space.queue_name("q", bad)


def test_space_itself_is_validated():
    with pytest.raises(ValueError, match="environment"):
        TopicSpace("prod/>", "apse1")
    with pytest.raises(ValueError, match="region"):
        TopicSpace("dev", "")


def test_queue_names_carry_environment_region_and_service(space):
    assert space.queue_name("q.cowork", "orchestrator") == "q.cowork.dev.apse1.orchestrator"


def test_parse_round_trips_events_and_commands(space):
    ev = space.parse("cowork/dev/apse1/orchestration/task/created/v1")
    assert ev == topics.TopicParts("dev", "apse1", "orchestration", "task", "created", "v1")
    assert not ev.is_command
    assert ev.event_type == "cowork.orchestration.task.created"
    cmd = space.parse("cowork/dev/apse1/agent/command/execute/v1")
    assert cmd.is_command
    assert cmd.event_type == "cowork.agent.command.execute"


@pytest.mark.parametrize(
    "topic",
    [
        "other/topic",
        "cowork/dev/apse1/orchestration/task/created",  # six levels
        "cowork/dev/apse1/orchestration/task/created/v1/extra",
        "cowork/prod/apse1/orchestration/task/created/v1",  # other environment
        "cowork/dev/euw1/orchestration/task/created/v1",  # other region
        "cowork/dev/apse1/orches*tion/task/created/v1",
    ],
)
def test_parse_rejects_foreign_or_malformed(space, topic):
    with pytest.raises(ValueError):
        space.parse(topic)
