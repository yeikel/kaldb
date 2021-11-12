package com.slack.kaldb.metadata.recovery;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import org.junit.Test;

public class RecoveryNodeMetadataTest {

  @Test
  public void testRecoveryNodeMetadata() {
    String name = "name";
    Metadata.RecoveryNodeMetadata.RecoveryNodeState state =
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE;
    String task = "task";
    long time = Instant.now().toEpochMilli();

    RecoveryNodeMetadata recoveryNodeMetadata = new RecoveryNodeMetadata(name, state, task, time);

    assertThat(recoveryNodeMetadata.name).isEqualTo(name);
    assertThat(recoveryNodeMetadata.recoveryNodeState).isEqualTo(state);
    assertThat(recoveryNodeMetadata.recoveryTaskName).isEqualTo(task);
    assertThat(recoveryNodeMetadata.updatedTimeUtc).isEqualTo(time);
  }

  @Test
  public void testRecoveryNodeEqualsHashcode() {
    String name = "name";
    Metadata.RecoveryNodeMetadata.RecoveryNodeState state =
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE;
    String task = "task";
    long time = Instant.now().toEpochMilli();

    RecoveryNodeMetadata recoveryNodeMetadataA = new RecoveryNodeMetadata(name, state, task, time);
    RecoveryNodeMetadata recoveryNodeMetadataB = new RecoveryNodeMetadata(name, state, task, time);
    RecoveryNodeMetadata recoveryNodeMetadataC =
        new RecoveryNodeMetadata(
            name, Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED, task, time);
    RecoveryNodeMetadata recoveryNodeMetadataD =
        new RecoveryNodeMetadata(name, state, "taskD", time);
    RecoveryNodeMetadata recoveryNodeMetadataE =
        new RecoveryNodeMetadata(name, state, task, time + 1);

    assertThat(recoveryNodeMetadataA).isEqualTo(recoveryNodeMetadataB);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataC);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataD);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataE);

    assertThat(recoveryNodeMetadataA.hashCode()).isEqualTo(recoveryNodeMetadataB.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataC.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataD.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataE.hashCode());
  }
}
