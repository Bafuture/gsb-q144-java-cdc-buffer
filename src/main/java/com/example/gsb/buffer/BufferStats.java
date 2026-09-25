package com.example.gsb.buffer;

/**
 * 缓冲区运行统计。
 *
 * @param writeCount             累计写入事件数
 * @param emitCount              累计产出（投递给下游）事件数，重试重投也计入；重放不计入
 * @param quarantinedCount       累计进入隔离区（死信）的事件数
 * @param backlog                当前积压量：已写入但尚未确认（含在途）的事件数
 * @param oldestUnackedSequence  最老未确认序号；无未确认事件时为 -1
 */
public record BufferStats(
        long writeCount,
        long emitCount,
        long quarantinedCount,
        long backlog,
        long oldestUnackedSequence) {
}
