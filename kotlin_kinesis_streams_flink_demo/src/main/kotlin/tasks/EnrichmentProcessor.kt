package org.example.tasks

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.TimerService
import org.example.configs.Configs
import org.example.models.AppUser
import org.example.models.AppUserWithRole
import org.example.models.UserRole
import org.apache.flink.util.Collector

class EnrichmentProcessor : KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>() {
    private lateinit var userState: ValueState<AppUser>
    private lateinit var roleState: ValueState<UserRole>
    private lateinit var timeoutState: ValueState<Long>

    override fun open(openContext: OpenContext) {
        userState = runtimeContext.getState(
            ValueStateDescriptor("users", AppUser::class.java)
        )
        roleState = runtimeContext.getState(
            ValueStateDescriptor("roles", UserRole::class.java)
        )
        timeoutState = runtimeContext.getState(
            ValueStateDescriptor("enrichment-timeout", Long::class.javaObjectType)
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
            out.collect(AppUserWithRole(value.username, value.email, role.role))
            clearPending(ctx.timerService())
        } else {
            scheduleTimeout(ctx)
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
            out.collect(AppUserWithRole(user.username, user.email, value.role))
            clearPending(ctx.timerService())
        } else {
            scheduleTimeout(ctx)
        }
    }

    override fun onTimer(
        timestamp: Long,
        ctx: KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>.OnTimerContext,
        out: Collector<AppUserWithRole>
    ) {
        // Ignore obsolete callbacks; only the currently pending record may expire.
        if (timeoutState.value() != timestamp) return

        val user = userState.value()
        val role = roleState.value()

        when {
            user != null && role != null -> {
                out.collect(AppUserWithRole(user.username, user.email, role.role))
            }

            user != null -> {
                out.collect(AppUserWithRole(user.username, user.email, null))
            }

            role != null -> {
                out.collect(AppUserWithRole(role.username, null, role.role))
            }
        }

        clearPending(ctx.timerService())
    }

    private fun scheduleTimeout(ctx: Context) {
        val eventTimestamp = requireNotNull(ctx.timestamp()) {
            "Enrichment requires event timestamps from the source"
        }
        // ValueState keeps the last arriving record; its event timestamp sets the deadline.
        timeoutState.value()?.let { ctx.timerService().deleteEventTimeTimer(it) }
        val deadline = eventTimestamp + Configs.ENRICHMENT_TIMEOUT_MS
        timeoutState.update(deadline)
        ctx.timerService().registerEventTimeTimer(deadline)
    }

    private fun clearPending(timers: TimerService) {
        timeoutState.value()?.let { timers.deleteEventTimeTimer(it) }
        timeoutState.clear()
        userState.clear()
        roleState.clear()
    }
}
