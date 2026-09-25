package com.example.gsb.buffer;

/**
 * 缓冲区统计快照。
 *
 * @param written                   累计写入事件数
 * @param delivered                 累计成功产出（被确认）事件数
 * @param deadLettered              累计进入隔离区（死信）事件数
 * @param backlog                   当前积压量：已写入但尚未确认也未隔离的事件数
 * @param oldestUnconfirmedSequence 最老未确认序号；无未确认事件时为 -1
 */
public record BufferStats(
        long written,
        long delivered,
        long deadLettered,
        long backlog,
        long oldestUnconfirmedSequence) {
}
