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
    }

    override fun processElement1(
        value: AppUser,
        ctx: KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>.Context,
        out: Collector<AppUserWithRole>
    ) {
        userState.update(value)
        out.collect(AppUserWithRole(value.username, value.email, roleState.value()?.role))
    }

    override fun processElement2(
        value: UserRole,
        ctx: KeyedCoProcessFunction<String, AppUser, UserRole, AppUserWithRole>.Context,
        out: Collector<AppUserWithRole>
    ) {
        roleState.update(value)
        out.collect(AppUserWithRole(value.username, userState.value()?.email, value.role))
    }
}
