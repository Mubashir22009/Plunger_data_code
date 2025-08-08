CREATE TABLE IF NOT EXISTS EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    cycle_id INTEGER NOT NULL,
    basic_pressure_event INTEGER,
    cycle_duration_event INTEGER,
    plunger_arrival_velocity_event INTEGER,
    gas_volume_produced_event INTEGER,
    --cycle_data_event INTEGER,
    unexpected_low_casing_pressure INTEGER,
    plunger_arrival_status_event INTEGER,
    plunger_unsafe_velocity_event INTEGER,
    unexpected_low_flow INTEGER,
    unexpected_low_cycle_duration INTEGER,
    unexpected_high_cycle_duration INTEGER,
    --cycle_anomaly_event INTEGER,
    FOREIGN KEY (basic_pressure_event) REFERENCES BASIC_PRESSURE_EVENTS(_id),
    FOREIGN KEY (cycle_duration_event) REFERENCES CYCLE_DURATION_EVENTS(_id),
    FOREIGN KEY (plunger_arrival_velocity_event) REFERENCES PLUNGER_ARRIVAL_VELOCITY_EVENTS(_id),
    FOREIGN KEY (gas_volume_produced_event) REFERENCES GAS_VOLUME_PRODUCED_EVENTS(_id),
    --FOREIGN KEY (cycle_data_event) REFERENCES CYCLE_DATA_EVENTS(_id),
    FOREIGN KEY (unexpected_low_casing_pressure) REFERENCES UNEXPECTED_LOW_CASING_PRESSURE_EVENTS(_id),
    FOREIGN KEY (plunger_arrival_status_event) REFERENCES PLUNGER_ARRIVAL_STATUS_EVENTS(_id),
    FOREIGN KEY (plunger_unsafe_velocity_event) REFERENCES PLUNGER_UNSAFE_VELOCITY_EVENTS(_id),
    FOREIGN KEY (unexpected_low_flow) REFERENCES UNEXPECTED_LOW_FLOW_EVENTS(_id),
    FOREIGN KEY (unexpected_low_cycle_duration) REFERENCES UNEXPECTED_LOW_CYCLE_DURATION_EVENTS(_id),
    FOREIGN KEY (unexpected_high_cycle_duration) REFERENCES UNEXPECTED_HIGH_CYCLE_DURATION_EVENTS(_id)
    --FOREIGN KEY (cycle_anomaly_event) REFERENCES CYCLE_ANOMALY_EVENTS(_id)
);

CREATE TABLE IF NOT EXISTS BASIC_PRESSURE_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    delta_pt FLOAT NOT NULL,
    delta_cp FLOAT NOT NULL,
    delta_pl FLOAT NOT NULL,
    ph FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS CYCLE_DURATION_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    total_duration INTEGER NOT NULL,
    flow_duration INTEGER NOT NULL,
    shutin_duration INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS PLUNGER_ARRIVAL_VELOCITY_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    arrival_speed FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS GAS_VOLUME_PRODUCED_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    gas_volume FLOAT NOT NULL,
    cycle_duration_event INTEGER NOT NULL,
    FOREIGN KEY (cycle_duration_event) REFERENCES CYCLE_DURATION_EVENTS(_id)
);

CREATE TABLE IF NOT EXISTS UNEXPECTED_LOW_CASING_PRESSURE_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    basic_pressure_event INTEGER NOT NULL,
    FOREIGN KEY (basic_pressure_event) REFERENCES BASIC_PRESSURE_EVENTS(_id)
);

CREATE TABLE IF NOT EXISTS PLUNGER_ARRIVAL_STATUS_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    non_arrival BOOLEAN NOT NULL,
    unexpected_casing_pressure BOOLEAN NOT NULL,
    unexpected_low_casing_pressure INTEGER,
    FOREIGN KEY (unexpected_low_casing_pressure) REFERENCES UNEXPECTED_LOW_CASING_PRESSURE_EVENTS(_id)
);

CREATE TABLE IF NOT EXISTS PLUNGER_UNSAFE_VELOCITY_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    velocity_event INTEGER NOT NULL,
    FOREIGN KEY (velocity_event) REFERENCES PLUNGER_ARRIVAL_VELOCITY_EVENTS(_id)
);

CREATE TABLE IF NOT EXISTS UNEXPECTED_LOW_FLOW_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    gas_volume_produced_event INTEGER NOT NULL,
    FOREIGN KEY (gas_volume_produced_event) REFERENCES GAS_VOLUME_PRODUCED_EVENTS(_id)
);

CREATE TABLE IF NOT EXISTS UNEXPECTED_LOW_CYCLE_DURATION_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    cycle_duration_event INTEGER NOT NULL,
    FOREIGN KEY (cycle_duration_event) REFERENCES CYCLE_DURATION_EVENTS(_id)
);

CREATE TABLE IF NOT EXISTS UNEXPECTED_HIGH_CYCLE_DURATION_EVENTS (
    _id INTEGER  NOT NULL PRIMARY KEY,
    cycle_duration_event INTEGER NOT NULL,
    FOREIGN KEY (cycle_duration_event) REFERENCES CYCLE_DURATION_EVENTS(_id)
);