package com.slack.kaldb.metadata.recovery;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.proto.metadata.Metadata;

import java.util.Objects;

/**
 * The recovery task metadata contains all information required to back-fill messages that have been
 * previously skipped. For partitionId, the recovery task should index from startOffset to
 * endOffset: [startOffset, endOffset].
 */
public class RecoveryTaskMetadata extends KaldbMetadata {
  public final String partitionId;
  public final long startOffset;
  public final long endOffset;
  public final long createdTimeEpochMs;
  public final Metadata.RecoveryTaskMetadata.RecoveryTaskState recoveryTaskState;

  public RecoveryTaskMetadata(
      String name, String partitionId, long startOffset, long endOffset, long createdTimeEpochMs, Metadata.RecoveryTaskMetadata.RecoveryTaskState recoveryTaskState) {
    super(name);

    checkArgument(
        partitionId != null && !partitionId.isEmpty(), "partitionId can't be null or empty");
    checkArgument(startOffset >= 0, "startOffset must greater than 0");
    checkArgument(
        endOffset >= startOffset, "endOffset must be greater than or equal to the startOffset");
    checkArgument(createdTimeEpochMs > 0, "createdTimeEpochMs must be greater than 0");
    checkArgument(recoveryTaskState != null, "recoveryTaskState can't be null");

    this.partitionId = partitionId;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.createdTimeEpochMs = createdTimeEpochMs;
    this.recoveryTaskState = recoveryTaskState;
  }

  public long getCreatedTimeEpochMs() {
    return createdTimeEpochMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RecoveryTaskMetadata that = (RecoveryTaskMetadata) o;
    return startOffset == that.startOffset && endOffset == that.endOffset && createdTimeEpochMs == that.createdTimeEpochMs && partitionId.equals(that.partitionId) && recoveryTaskState == that.recoveryTaskState;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partitionId, startOffset, endOffset, createdTimeEpochMs, recoveryTaskState);
  }

  @Override
  public String toString() {
    return "RecoveryTaskMetadata{" +
        "partitionId='" + partitionId + '\'' +
        ", startOffset=" + startOffset +
        ", endOffset=" + endOffset +
        ", createdTimeEpochMs=" + createdTimeEpochMs +
        ", recoveryTaskState=" + recoveryTaskState +
        ", name='" + name + '\'' +
        '}';
  }
}
