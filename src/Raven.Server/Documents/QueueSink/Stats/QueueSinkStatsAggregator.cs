﻿using System;
using System.Diagnostics;
using Raven.Server.Documents.QueueSink.Stats.Performance;
using Raven.Server.Utils.Stats;
using Sparrow;
using Size = Raven.Client.Util.Size;

namespace Raven.Server.Documents.QueueSink.Stats;

public class QueueSinkStatsAggregator : StatsAggregator<QueueSinkRunStats, QueueSinkStatsScope>
{
    private volatile QueueSinkPerformanceStats _performanceStats;

    public QueueSinkStatsAggregator(int id, IStatsAggregator lastStats) : base(id, lastStats)
    {
    }

    public override QueueSinkStatsScope CreateScope()
    {
        Debug.Assert(Scope == null);

        return Scope = new QueueSinkStatsScope(Stats);
    }

    public QueueSinkPerformanceStats ToPerformanceStats()
    {
        if (_performanceStats != null)
            return _performanceStats;

        lock (Stats)
        {
            if (_performanceStats != null)
                return _performanceStats;

            return _performanceStats = CreatePerformanceStats(completed: true);
        }
    }

    public QueueSinkPerformanceStats ToPerformanceLiveStatsWithDetails()
    {
        if (_performanceStats != null)
            return _performanceStats;

        if (Scope == null || Stats == null)
            return null;

        if (Completed)
            return ToPerformanceStats();

        return CreatePerformanceStats(completed: false);
    }

    public QueueSinkPerformanceStats ToPerformanceLiveStats()
    {
        throw new System.NotImplementedException();
    }

    private QueueSinkPerformanceStats CreatePerformanceStats(bool completed)
    {
        return new QueueSinkPerformanceStats(Scope.Duration)
        {
            Id = Id,
            Started = StartTime,
            Completed = completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
            Details = Scope.ToQueueSinkPerformanceOperation("Queue Sink"),
            NumberOfPulledMessages = Stats.NumberOfPulledMessages,
            NumberOfProcessedMessages = Stats.NumberOfProcessedMessages,
            ScriptErrorCount = Stats.ScriptErrorCount,
            SuccessfullyProcessed = Stats.SuccessfullyProcessed,
            BatchPullStopReason = Stats.BatchPullStopReason,
            CurrentlyAllocated = new Size(Stats.CurrentlyAllocated.GetValue(SizeUnit.Bytes)),
        };
    }
}
