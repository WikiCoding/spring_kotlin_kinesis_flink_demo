package org.example.tasks

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.example.configs.Configs
import org.example.models.AppUser
import org.example.models.AppUserWithRole
import org.example.models.UserRole
import java.time.Duration

class EnrichmentProcessor : KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>() {
    private lateinit var userState: ValueState<AppUser>
    private lateinit var roleState: ValueState<UserRole>
    private lateinit var pendingTimerState: ValueState<Long>

    override fun open(openContext: OpenContext) {
        val ttl = StateTtlConfig.newBuilder(Duration.ofHours(Configs.STATE_TTL_DURATION_HOURS))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()

        userState = runtimeContext.getState(
            ValueStateDescriptor("users", AppUser::class.java)
                .apply { enableTimeToLive(ttl) }
        )
        roleState = runtimeContext.getState(
            ValueStateDescriptor("roles", UserRole::class.java)
                .apply { enableTimeToLive(ttl) }
        )
        pendingTimerState = runtimeContext.getState(
            ValueStateDescriptor("pending-timer", Long::class.javaObjectType)
        )
    }

    override fun processElement1(
        value: AppUser,
        ctx: KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>.Context,
        out: Collector<AppUserWithRole>
    ) {
        userState.update(value)

        val role = roleState.value()
        if (role != null) {
            cancelPendingTimer(ctx)
            out.collect(AppUserWithRole(value.username, value.email, role.role))
        } else {
            startPendingTimer(ctx)
        }
    }

    override fun processElement2(
        value: UserRole,
        ctx: KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>.Context,
        out: Collector<AppUserWithRole>
    ) {
        roleState.update(value)

        val user = userState.value()
        if (user != null) {
            cancelPendingTimer(ctx)
            out.collect(AppUserWithRole(user.username, user.email, value.role))
        } else {
            startPendingTimer(ctx)
        }
    }

    override fun onTimer(
        timestamp: Long,
        ctx: KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>.OnTimerContext,
        out: Collector<AppUserWithRole>
    ) {
        pendingTimerState.clear()

        // Only one side arrived within the window: emit it incomplete.
        val user = userState.value()
        val role = roleState.value()
        when {
            user != null -> out.collect(AppUserWithRole(user.username, user.email, null))
            role != null -> out.collect(AppUserWithRole(role.username, null, role.role))
        }
    }

    private fun startPendingTimer(
        ctx: KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>.Context
    ) {
        if (pendingTimerState.value() != null) return

        val deadline = ctx.timerService().currentProcessingTime() + Configs.ENRICHMENT_TIMEOUT_MS
        pendingTimerState.update(deadline)
        ctx.timerService().registerProcessingTimeTimer(deadline)
    }

    private fun cancelPendingTimer(
        ctx: KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>.Context
    ) {
        pendingTimerState.value()?.let { ctx.timerService().deleteProcessingTimeTimer(it) }
        pendingTimerState.clear()
    }
}