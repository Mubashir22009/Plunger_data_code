
class EventsGenerator:
    def __init__(self, data_loader, database):
        self.data_loader = data_loader
        self.df = data_loader.load()
        print("ret from load")
        self.database = database

        # Ensure the database is ready
        if not self.database.connection:
            raise ValueError("Database connection is not established.")

    def generate_events(self):
        return self.__generate_events_per_cycle(
            basic_event_funcs=[
                self.__BasicPressureEvent,
                self.__CycleDurationEvent,
                self.__PlungerArrivalVelocityEvent
            ],
            complex_event_funcs=[
                self.__GasVolumeProducedEvent,
                # self.__CycleDataEvent,
                self.__UnexpectedLowCasingPressure,
                self.__PlungerArrivalStatusEvent,
                self.__PlungerUnsafeVelocityEvent,
                self.__UnexpectedLowFlow,
                self.__UnexpectedLowCycleDuration,
                self.__UnexpectedHighCycleDuration,
                # self.__CycleAnomalyEvent
            ]
        )


    def __generate_events_per_cycle(self, basic_event_funcs, complex_event_funcs):
        for cycle_id, group in self.df.groupby('cycle_id'):
            cycle_events = {}
            for event_func in basic_event_funcs:
                if callable(event_func):
                    id, name = event_func(group)
                    cycle_events[name] = id
            for event_func in complex_event_funcs:
                if callable(event_func):
                    id, name = event_func(group, cycle_events)
                    cycle_events[name] = id

            # Filter out events with None as id or name
            filtered_events = {k: v for k, v in cycle_events.items() if v is not None}
            parent_event = {
                "name": "EVENTS",
                "cycle_id": int(cycle_id),
                **filtered_events
            }
            self.database.insertEvent(parent_event)
    
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

        id = self.database.insertEvent(
            {
                "name": "BASIC_PRESSURE_EVENTS",
                "delta_pt": round(float(delta_Pt), 3),
                "delta_cp": round(float(delta_Cp), 3),
                "delta_pl": round(float(delta_Pl), 3),
                "ph": round(float(ph), 3)
            }
        )

        return id, 'basic_pressure_event'
    
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

        id = self.database.insertEvent(
            {
                "name": "CYCLE_DURATION_EVENTS",
                "start_time": start_time,
                "end_time": end_time,
                "total_duration": total_duration,
                "flow_duration": flow_duration,
                "shutin_duration": shutin_duration
            }
        )
        return id, 'cycle_duration_event'

    def __PlungerArrivalVelocityEvent(self, cycle_df):
        # Convert Arrival Speed.csv to float and get mean arrival speed for the cycle
        arrival_speed = cycle_df["Arrival Speed.csv"].astype(float).mean()
        id = self.database.insertEvent(
            {
                "name": "PLUNGER_ARRIVAL_VELOCITY_EVENTS",
                "arrival_speed": round(float(arrival_speed), 3)  # m/s
            }
        )
    
        return id, 'plunger_arrival_velocity_event'
    
    # == Complex Events ==

    def __GasVolumeProducedEvent(self, cycle_df, events):
        cycle_duration_id = events.get('cycle_duration_event')
        cycle_duration_event = self.database.fetch_event(cycle_duration_id, "CYCLE_DURATION_EVENTS")

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

        id = self.database.insertEvent(
            {
                "name": "GAS_VOLUME_PRODUCED_EVENTS",
                "gas_volume": round(float(gas_volume), 3),  # m³
                "cycle_duration_event": cycle_duration_id,
            }
        )
        
        return id, 'gas_volume_produced_event'
    
    # def __CycleDataEvent(self, cycle_df, events):
    #     event_log = {
    #         "cycle_id": int(cycle_df.iloc[0]["cycle_id"])
    #     }
    #     # Map event names to their data
    #     for event in events:
    #         if "BasicPressureEvent" in event:
    #             event_log["basicPressureEvent"] = event["BasicPressureEvent"]
    #         elif "CycleDurationEvent" in event:
    #             event_log["cycleDurationEvent"] = event["CycleDurationEvent"]
    #         elif "GasVolumeProducedEvent" in event:
    #             event_log["gasVolumeProduced"] = event["GasVolumeProducedEvent"]
    #         elif "PlungerArrivalVelocityEvent" in event:
    #             event_log["velocityEvent"] = event["PlungerArrivalVelocityEvent"]
    #     return {"CycleDataEvent": event_log}

    def __UnexpectedLowCasingPressure(self, cycle_df, events, threshold=-5.0):
        basic_pressure_event_id = events.get('basic_pressure_event')
        basic_pressure_event = self.database.fetch_event(basic_pressure_event_id, "BASIC_PRESSURE_EVENTS")

        delta_cp = basic_pressure_event['delta_cp']
        if delta_cp < threshold:
            id = self.database.insertEvent(
                {
                    "name": "UNEXPECTED_LOW_CASING_PRESSURE_EVENTS",
                    "basic_pressure_event": basic_pressure_event_id,
                    # "description": "Abnormally low casing pressure change detected."
                }
            )
            return id, 'unexpected_low_casing_pressure'
        else:
            # No event triggered
            return None, None

    def __PlungerArrivalStatusEvent(self, cycle_df, events):
        # non_arrival: True if all Current Non-Arrival Count.csv > 0, else False
        non_arrival = cycle_df["Current Non-Arrival Count.csv"].astype(float).max() > 0

        unexpected_casing_pressure_id = events.get('unexpected_low_casing_pressure', None)

        if unexpected_casing_pressure_id is not None:
            id = self.database.insertEvent(
            {
                "name": "PLUNGER_ARRIVAL_STATUS_EVENTS",
                "non_arrival": bool(non_arrival),
                "unexpected_casing_pressure": True,
                "unexpected_low_casing_pressure": unexpected_casing_pressure_id
            }
        )
        else:
            id = self.database.insertEvent(
                {
                    "name": "PLUNGER_ARRIVAL_STATUS_EVENTS",
                    "non_arrival": bool(non_arrival),
                    "unexpected_casing_pressure": False,
                }
            )

        return id, 'plunger_arrival_status_event'
    
    def __PlungerUnsafeVelocityEvent(self, cycle_df, events, safety_threshold=2.5):
        plunger_arrival_velocity_id = events.get('plunger_arrival_velocity_event')

        velocity_event = self.database.fetch_event(plunger_arrival_velocity_id, "PLUNGER_ARRIVAL_VELOCITY_EVENTS")
        arrival_speed = velocity_event["arrival_speed"]
        unsafe = arrival_speed > safety_threshold

        if unsafe:
            id = self.database.insertEvent(
                {
                    "name": "PLUNGER_UNSAFE_VELOCITY_EVENTS",
                    "velocity_event": plunger_arrival_velocity_id,
                }
            )
            return id, 'plunger_unsafe_velocity_event'
        return None, None
    
    def __UnexpectedLowFlow(self, cycle_df, events, volume_threshold=10.0):
        gas_volume_event_id = events.get('gas_volume_produced_event')

        gas_volume_event = self.database.fetch_event(gas_volume_event_id, "GAS_VOLUME_PRODUCED_EVENTS")
        gas_volume = gas_volume_event["gas_volume"]
        if gas_volume < volume_threshold:
            id = self.database.insertEvent(
                {
                    "name": "UNEXPECTED_LOW_FLOW_EVENTS",
                    "gas_volume_produced_event": gas_volume_event_id,
                }
            )
            return id, 'unexpected_low_flow'
        return None, None

    def __UnexpectedLowCycleDuration(self, cycle_df, events, total_duration_threshold=600, flow_duration_threshold=300, shutin_duration_threshold=300):
        cycle_duration_event_id = events.get('cycle_duration_event')
        if not cycle_duration_event_id:
            return None, None

        cycle_duration_event = self.database.fetch_event(cycle_duration_event_id, "CYCLE_DURATION_EVENTS")
        total_duration = cycle_duration_event["total_duration"]
        flow_duration = cycle_duration_event["flow_duration"]
        shutin_duration = cycle_duration_event["shutin_duration"]

        is_short = (
            total_duration < total_duration_threshold or
            flow_duration < flow_duration_threshold or
            shutin_duration < shutin_duration_threshold
        )

        if is_short:
            id = self.database.insertEvent(
                {
                    "name": "UNEXPECTED_LOW_CYCLE_DURATION_EVENTS",
                    "cycle_duration_event": cycle_duration_event_id,
                }
            )
            return id, 'unexpected_low_cycle_duration'
        return None, None
    
    def __UnexpectedHighCycleDuration(self, cycle_df, events, total_duration_threshold=7200, flow_duration_threshold=3600, shutin_duration_threshold=3600):
        cycle_duration_event_id = events.get('cycle_duration_event')
        if not cycle_duration_event_id:
            return None, None

        cycle_duration_event = self.database.fetch_event(cycle_duration_event_id, "CYCLE_DURATION_EVENTS")
        total_duration = cycle_duration_event["total_duration"]
        flow_duration = cycle_duration_event["flow_duration"]
        shutin_duration = cycle_duration_event["shutin_duration"]

        is_long = (
            total_duration > total_duration_threshold or
            flow_duration > flow_duration_threshold or
            shutin_duration > shutin_duration_threshold
        )

        if is_long:
            id = self.database.insertEvent(
                {
                    "name": "UNEXPECTED_HIGH_CYCLE_DURATION_EVENTS",
                    "cycle_duration_event": cycle_duration_event_id,
                }
            )
            return id, 'unexpected_high_cycle_duration'
        return None, None
    
    # def __CycleAnomalyEvent(self, cycle_df, events):
    #     # Map anomaly event keys to their output names and descriptions
    #     anomaly_map = {
    #         "PlungerUnsafeVelocity": "plungerUnsafeVelocity",
    #         "PlungerNonArrival": "plungerNonArrival",
    #         "UnexpectedLowCasingPressure": "UnexpectedCasingPressure",
    #         "UnexpectedLowFlow": "UnexpectedLowFlow",
    #         "UnexpectedLowCycleDuration": "UnexpectedLowCycleDuration",
    #         "UnexpectedHighCycleDuration": "UnexpectedHighCycleDuration"
    #     }

    #     # Find which anomaly events are present in the events list
    #     triggered = {}
    #     for event in events:
    #         for key, output_name in anomaly_map.items():
    #             if key in event:
    #                 triggered[output_name] = event[key]

    #     if triggered:
    #         return {
    #             "CycleAnomalyEvent": {
    #                 "cycle_id": int(cycle_df.iloc[0]["cycle_id"]),
    #                 "anomalies": list(triggered.keys()),
    #                 "details": triggered,
    #                 "description": (
    #                     "The CycleAnomalyEvent is a composite event designed to encapsulate critical failures, "
    #                     "inefficiencies, or safety violations occurring during a plunger lift cycle. It is triggered "
    #                     "whenever one or more constituent anomaly events occur, including PlungerNonArrival, "
    #                     "PlungerUnsafeVelocity, UnexpectedLowCasingPressure, UnexpectedLowFlow, "
    #                     "UnexpectedLowCycleDuration, and UnexpectedHighCycleDuration. This wrapper event provides "
    #                     "a high-level signal indicating that a cycle has deviated from expected operational behavior. "
    #                     "It supports automated monitoring and root-cause analysis by summarizing which specific "
    #                     "anomalies occurred, helping identify equipment issues, cycle misconfigurations, or poor lift "
    #                     "conditions that warrant intervention."
    #                 )
    #             }
    #         }
    #     return None