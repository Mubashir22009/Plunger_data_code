
class EventsGenerator:
    def __init__(self, data_loader):
        self.data_loader = data_loader
        self.df = data_loader.load()

    def generate_events(self):
        return self.__generate_events_per_cycle(
            basic_event_funcs=[
                self.__BasicPressureEvent,
                self.__CycleDurationEvent,
                self.__PlungerArrivalVelocityEvent
            ],
            complex_event_funcs=[
                self.__GasVolumeProducedEvent,
                self.__CycleDataEvent,
                self.__UnexpectedLowCasingPressure,
                self.__PlungerArrivalStatusEvent,
                self.__PlungerUnsafeVelocityEvent,
                self.__UnexpectedLowFlow,
                self.__UnexpectedLowCycleDuration,
                self.__UnexpectedHighCycleDuration,
                self.__CycleAnomalyEvent
            ]
        )


    def __generate_events_per_cycle(self, basic_event_funcs, complex_event_funcs):
        events = []
        for _, group in self.df.groupby('cycle_id'):
            basic_cycle_events = []
            for event_func in basic_event_funcs:
                if callable(event_func):
                    basic_cycle_events.append(event_func(group))
            complex_cycle_events = []
            for event_func in complex_event_funcs:
                if callable(event_func):
                    complex_cycle_events.append(event_func(group, basic_cycle_events))

            events.extend(basic_cycle_events)
            events.extend(complex_cycle_events)
        return events
    
    # == Basic Events ==

    def __BasicPressureEvent(self, cycle_df, SG=0.6, hl=1000):
        cols = [
            "Tubing Pressure (PSI).csv",
            "Casing Pressure (PSI).csv",
            "Line Pressure (PSIA).csv"
        ]
        for col in cols:
            cycle_df[col] = cycle_df[col].astype(float)

        Pt_init = cycle_df.iloc[0]["Tubing Pressure (PSI).csv"]
        Pt_final = cycle_df.iloc[-1]["Tubing Pressure (PSI).csv"]
        Cp_init = cycle_df.iloc[0]["Casing Pressure (PSI).csv"]
        Cp_final = cycle_df.iloc[-1]["Casing Pressure (PSI).csv"]
        Pl_init = cycle_df.iloc[0]["Line Pressure (PSIA).csv"]
        Pl_final = cycle_df.iloc[-1]["Line Pressure (PSIA).csv"]

        delta_Pt = Pt_final - Pt_init
        delta_Cp = Cp_final - Cp_init
        delta_Pl = Pl_final - Pl_init
        ph = 0.433 * SG * hl

        return {"BasicPressureEvent": {
            "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
            "delta_Pt": round(float(delta_Pt), 3),
            "delta_Cp": round(float(delta_Cp), 3),
            "delta_Pl": round(float(delta_Pl), 3),
            "ph": round(float(ph), 3)
        }}
    
    def __CycleDurationEvent(self, cycle_df):
        # Start and end time of the cycle
        start_time = int(cycle_df.iloc[0]['isotime'])
        end_time = int(cycle_df.iloc[-1]['isotime'])
        total_duration = end_time - start_time

        # Flow duration: time when flow_rate was not zero
        non_zero = cycle_df[cycle_df['flow_rate'] > 0]
        if not non_zero.empty:
            flow_start = int(non_zero.iloc[0]['isotime'])
            flow_end = int(non_zero.iloc[-1]['isotime'])
            flow_duration = flow_end - flow_start
        else:
            flow_duration = 0

        # Shutin duration: time when flow_rate was zero
        zero = cycle_df[cycle_df['flow_rate'] == 0]
        if not zero.empty:
            shutin_start = int(zero.iloc[0]['isotime'])
            shutin_end = int(zero.iloc[-1]['isotime'])
            shutin_duration = shutin_end - shutin_start
        else:
            shutin_duration = 0

        return {"CycleDurationEvent": {
            "cycle_id": int(cycle_df.iloc[0]['cycle_id']),
            "start_time": start_time,
            "end_time": end_time,
            "total_duration": total_duration,
            "flow_duration": flow_duration,
            "shutin_duration": shutin_duration
        }}

    def __PlungerArrivalVelocityEvent(self, cycle_df):
        # Convert Arrival Speed.csv to float and get mean arrival speed for the cycle
        arrival_speed = cycle_df["Arrival Speed.csv"].astype(float).mean()
        return {
            "PlungerArrivalVelocityEvent": {
                "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
                "arrival_speed": round(float(arrival_speed), 3),  # m/s
            }
        }
    
    # == Complex Events ==

    def __GasVolumeProducedEvent(self, cycle_df, events):
        # Find the CycleDurationEvent in the provided events
        cycle_duration_event = next((e["CycleDurationEvent"] for e in events if "CycleDurationEvent" in e), None)
        if cycle_duration_event is None:
            return {"GasVolumeProducedEvent": None}

        flow_duration = cycle_duration_event["flow_duration"]  # in seconds
        # Use mean flow_rate during the cycle (excluding zeros)
        non_zero_flow = cycle_df[cycle_df['flow_rate'] > 0]
        if not non_zero_flow.empty:
            avg_flow_rate = non_zero_flow['flow_rate'].mean()  # MCF/Day
            # Convert avg_flow_rate from MCF/Day to cubic meters/second
            # 1 MCF = 28.3168 m³, 1 day = 86400 seconds
            avg_flow_rate_m3s = avg_flow_rate * 28.3168 / 86400
            gas_volume = avg_flow_rate_m3s * flow_duration  # in cubic meters
        else:
            gas_volume = 0.0

        return {
            "GasVolumeProducedEvent": {
                "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
                "gas_volume": round(float(gas_volume), 3),  # cubic meters
                "CycleDurationEvent": cycle_duration_event  # in seconds
            }
        }
    
    def __CycleDataEvent(self, cycle_df, events):
        event_log = {
            "cycle_id": int(cycle_df.iloc[0]["cycle_id"])
        }
        # Map event names to their data
        for event in events:
            if "BasicPressureEvent" in event:
                event_log["basicPressureEvent"] = event["BasicPressureEvent"]
            elif "CycleDurationEvent" in event:
                event_log["cycleDurationEvent"] = event["CycleDurationEvent"]
            elif "GasVolumeProducedEvent" in event:
                event_log["gasVolumeProduced"] = event["GasVolumeProducedEvent"]
            elif "PlungerArrivalVelocityEvent" in event:
                event_log["velocityEvent"] = event["PlungerArrivalVelocityEvent"]
        return {"CycleDataEvent": event_log}

    def __UnexpectedLowCasingPressure(self, cycle_df, events, threshold=-5.0):
        basic_pressure_event = next((e["BasicPressureEvent"] for e in events if "BasicPressureEvent" in e))

        delta_cp = basic_pressure_event['delta_Cp']
        if delta_cp < threshold:
            return {
                "UnexpectedLowCasingPressure": {
                    "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
                    "BasicPressureEvent": basic_pressure_event,
                    "description": "Abnormally low casing pressure change detected."
                }
            }
        return None

    def __PlungerArrivalStatusEvent(self, cycle_df, events):
        # non_arrival: True if all Current Non-Arrival Count.csv > 0, else False
        non_arrival = cycle_df["Current Non-Arrival Count.csv"].astype(float).max() > 0

        unexpected_casing_pressure = next((e["UnexpectedLowCasingPressure"] for e in events if "UnexpectedLowCasingPressure" in e), None)

        return {
            "PlungerArrivalStatusEvent": {
                "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
                "non_arrival": bool(non_arrival),
                "unexpected_casing_pressure": bool(unexpected_casing_pressure),
                "UnexpectedLowCasingPressure": unexpected_casing_pressure,
                "description": (
                    "Plunger did not arrive; " if non_arrival else "Plunger arrived; "
                ) + (
                    "Unexpected low casing pressure detected."
                    if unexpected_casing_pressure
                    else "Casing pressure normal."
                ),
            }
        }
    
    def __PlungerUnsafeVelocityEvent(self, cycle_df, events, safety_threshold=2.5):
        # Find the PlungerArrivalVelocityEvent in the provided events
        velocity_event = next((e["PlungerArrivalVelocityEvent"] for e in events if "PlungerArrivalVelocityEvent" in e), None)
        if velocity_event is None:
            return None

        arrival_speed = velocity_event["arrival_speed"]
        unsafe = arrival_speed > safety_threshold

        if unsafe:
            return {
                "PlungerUnsafeVelocity": {
                    "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
                    "arrival_speed": arrival_speed,
                    "velocityEvent": velocity_event,
                    "description": (
                        "The PlungerUnsafeVelocity event is triggered when the velocity of the plunger upon arrival "
                        "at the surface exceeds a predefined safety threshold. This indicates potentially dangerous "
                        "impact forces that could damage equipment or signal aggressive flow conditions. "
                        "Operators should consider adjusting flow duration, shut-in pressure, or inspect for mechanical wear."
                    ),
                    "safety_threshold": safety_threshold
                }
            }
        return None
    
    def __UnexpectedLowFlow(self, cycle_df, events, volume_threshold=10.0):
        gas_volume_event = next((e["GasVolumeProducedEvent"] for e in events if "GasVolumeProducedEvent" in e), None)
        if gas_volume_event is None:
            return None

        gas_volume = gas_volume_event["gas_volume"]
        if gas_volume < volume_threshold:
            return {
                "UnexpectedLowFlow": {
                    "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
                    "gas_volume": gas_volume,
                    "volume_threshold": volume_threshold,
                    "description": (
                        "The UnexpectedLowFlow event is raised when the gas volume produced during a cycle is significantly lower than expected. "
                        "This may indicate underperformance, early plunger fallback, or poor liquid unloading."
                    ),
                }
            }
        return None

    def __UnexpectedLowCycleDuration(self, cycle_df, events, total_duration_threshold=600, flow_duration_threshold=300, shutin_duration_threshold=300):
        # Find the CycleDurationEvent in the provided events
        cycle_duration_event = next((e["CycleDurationEvent"] for e in events if "CycleDurationEvent" in e), None)
        if cycle_duration_event is None:
            return None

        total_duration = cycle_duration_event["total_duration"]
        flow_duration = cycle_duration_event["flow_duration"]
        shutin_duration = cycle_duration_event["shutin_duration"]

        is_short = (
            total_duration < total_duration_threshold or
            flow_duration < flow_duration_threshold or
            shutin_duration < shutin_duration_threshold
        )

        if is_short:
            return {
                "UnexpectedLowCycleDuration": {
                    "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
                    "total_duration": total_duration,
                    "flow_duration": flow_duration,
                    "shutin_duration": shutin_duration,
                    "thresholds": {
                        "total_duration": total_duration_threshold,
                        "flow_duration": flow_duration_threshold,
                        "shutin_duration": shutin_duration_threshold
                    },
                    "CycleDurationEvent": cycle_duration_event,
                    "description": (
                        "The UnexpectedLowCycleDuration event flags abnormally short cycles, either in total duration or during specific segments like flow or shut-in. "
                        "Such a condition could suggest premature venting, shallow slug formation, or mistimed plunger launches. "
                        "Identifying low-duration anomalies is important to optimize cycle timing and avoid inefficient runtimes."
                    )
                }
            }
        return None
    
    def __UnexpectedHighCycleDuration(self, cycle_df, events, total_duration_threshold=7200, flow_duration_threshold=3600, shutin_duration_threshold=3600):
        cycle_duration_event = next((e["CycleDurationEvent"] for e in events if "CycleDurationEvent" in e), None)
        if cycle_duration_event is None:
            return None

        total_duration = cycle_duration_event["total_duration"]
        flow_duration = cycle_duration_event["flow_duration"]
        shutin_duration = cycle_duration_event["shutin_duration"]

        is_long = (
            total_duration > total_duration_threshold or
            flow_duration > flow_duration_threshold or
            shutin_duration > shutin_duration_threshold
        )

        if is_long:
            return {
                "UnexpectedHighCycleDuration": {
                    "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
                    "total_duration": total_duration,
                    "flow_duration": flow_duration,
                    "shutin_duration": shutin_duration,
                    "thresholds": {
                        "total_duration": total_duration_threshold,
                        "flow_duration": flow_duration_threshold,
                        "shutin_duration": shutin_duration_threshold
                    },
                    "CycleDurationEvent": cycle_duration_event,
                    "description": (
                        "The UnexpectedHighCycleDuration event highlights cycles that exceed acceptable time limits, "
                        "particularly during flow or shut-in phases. This can indicate poor liquid unloading, sluggish arrival, "
                        "excessive shut-in, or gas buildup delays. By identifying these long cycles, operators can rebalance lift "
                        "frequency, flow durations, and shut-in strategies to restore cycle efficiency."
                    )
                }
            }
        return None
    
    def __CycleAnomalyEvent(self, cycle_df, events):
        # Map anomaly event keys to their output names and descriptions
        anomaly_map = {
            "PlungerUnsafeVelocity": "plungerUnsafeVelocity",
            "PlungerNonArrival": "plungerNonArrival",
            "UnexpectedLowCasingPressure": "UnexpectedCasingPressure",
            "UnexpectedLowFlow": "UnexpectedLowFlow",
            "UnexpectedLowCycleDuration": "UnexpectedLowCycleDuration",
            "UnexpectedHighCycleDuration": "UnexpectedHighCycleDuration"
        }

        # Find which anomaly events are present in the events list
        triggered = {}
        for event in events:
            for key, output_name in anomaly_map.items():
                if key in event:
                    triggered[output_name] = event[key]

        if triggered:
            return {
                "CycleAnomalyEvent": {
                    "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
                    "anomalies": list(triggered.keys()),
                    "details": triggered,
                    "description": (
                        "The CycleAnomalyEvent is a composite event designed to encapsulate critical failures, "
                        "inefficiencies, or safety violations occurring during a plunger lift cycle. It is triggered "
                        "whenever one or more constituent anomaly events occur, including PlungerNonArrival, "
                        "PlungerUnsafeVelocity, UnexpectedLowCasingPressure, UnexpectedLowFlow, "
                        "UnexpectedLowCycleDuration, and UnexpectedHighCycleDuration. This wrapper event provides "
                        "a high-level signal indicating that a cycle has deviated from expected operational behavior. "
                        "It supports automated monitoring and root-cause analysis by summarizing which specific "
                        "anomalies occurred, helping identify equipment issues, cycle misconfigurations, or poor lift "
                        "conditions that warrant intervention."
                    )
                }
            }
        return None