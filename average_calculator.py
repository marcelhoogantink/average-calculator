from collections import deque
from datetime import datetime, timedelta
from typing import Optional,Any
import bisect

log.info("Loading Average Calculator pyscript....")

## Time-weighted average calculator for Home Assistant pyscript.
def dummy():
    """Start with this automation.

    alias: Start Pyscript Average Calculator
    description: Starting Pyscript Average Calculator on HA start or pyscript reload.
    triggers:
    - trigger: homeassistant
        event: start
    - trigger: event
        event_type: average_calculator_reloaded
    conditions: []
    actions:
    - action: pyscript.start_average_calculator
        metadata: {}
        data: {}
    mode: single

    """

DEBUG = False

MAIN_SENSORS = [
    # SENSORS: (source, avg_target_suffix, energy_target_suffix[, mode, threshold])
    ("sensor.power_consumers", "_avg_min", None, "step"),
    ("sensor.envoy_122202147358_current_power_production", "_avg_min", None, "step"),
    ("sensor.p1_meter_power", "_avg_min", None, "step"),
#    ("sensor.p1_meter_vermogen", "_avg_min", None, "step"),
#    ("sensor.ct_meters_zonnepanelen_power_ab", "_avg_min", None, "step"),
]

DEFAULT_MODE = "step"   # options: "step", "linear", "linear_extrapolate"
DEFAULT_THRESHOLD = None         # W minimum before treated as 0

BUFFER_MARGIN = timedelta(seconds=5)
MAX_POINTS = 300          # safety cap on points stored per sensor

SENSOR_IDS = [s[0] for s in MAIN_SENSORS]

triggers = {}
data = {}
sources = []
groups = []
group_restored_list = []
group_entity_list = {}
group_info = {}


def is_valid(entity):

    try:
        val = state.get(entity) or None

    except (NameError):
        return False


    return val not in (None, "unknown", "unavailable")


#
# Wait until all sensors are available, THEN run your logic
#
@service
async def start_average_calculator():

    log.info("Waiting Average Calculator...")

    # Loop until ALL sensors report a valid state
    count=0
    while ( count < 10):
        count+=1
        missing = [s for s in SENSOR_IDS if not is_valid(s)]

        if not missing:
            log.info("All MAIN_SENSORS are available — starting main processing.")
            break

        log.info(f"Waiting for sensors to become available: {missing}/ {count}  ")
        await task.sleep(1)

    if (missing):
        log.warning(f"Some sensors are still missing after wait: {missing}.")
        log.error(f"Average Calculator setup Stopped. (missing sensors: {missing})")
    else:
    #
    # Now that sensors are ready, run your function
    #
        log.info("Running Average Calculator...")
        main_average_calculator()

# MAIN CODE below !!

###################
# === FUNCTIONS ===
###################
def _log(msg: str):
    if DEBUG:
        log.info(msg)

def add_value(src: str, t: datetime, v: float) -> None:
    values = data[src]["values"]

    # Fast path: append if list is empty or t >= last timestamp
    if not values or t >= values[-1][0]:
        values.append((t, v))
    else:
        # Insert while maintaining order
        bisect.insort(values, (t, v))

def state_get(sensor):
    try:
        var = state.get(sensor)
    except (NameError,AttributeError):
        return None

    return var

def set_data(main_sensor,sensor):
    state_value=state.get(sensor)
    if state_value in ("unknown", "unavailable", "none"):
        log.warning(f"Source {sensor} state is {state_value} at startup, initializing with 0.0")
        state_value = 0.0
    _log(f"Test value for {sensor}: {state_value}")
    time=state.get(sensor+".last_updated").replace(tzinfo=None)

    mode = group_info[main_sensor]["mode"]
    threshold = group_info[main_sensor]["threshold"]
    friendly_name=state.get(sensor+".friendly_name")
    unit_of_measurement=state.get(sensor+".unit_of_measurement")
    device_class=state_get(sensor+".device_class")

    data[sensor] = {
        "values": deque(maxlen=MAX_POINTS),  # stores (datetime, float)
        "last_value": None,                   # (datetime, float)
        "mode": mode,
        "threshold": threshold,
        "friendly_name": friendly_name,
        "unit_of_measurement": unit_of_measurement,
        "device_class": device_class,
        "avg_suffix": group_info[main_sensor]["avg_suffix"],
        "energy_suffix": group_info[main_sensor]["energy_suffix"],
    }

    _log(f"Initializing {sensor} with last known value: {state_value} at {time}, {friendly_name}, {unit_of_measurement}")
    add_value(sensor, time, float(state_value))

def state_trigger_factory(source):

    log.info(f"Creating state triggers for sources: {source}")
    @state_trigger(source)
    def state_trigger_var(value=None, old_value=None, trigger_type=None, var_name=None):
        triggered_at = datetime.now()
        src = var_name

        if value is None:
            return

        try:
            v = float(value)
        except (TypeError, ValueError):
            return

        v = apply_threshold(src, v)
        data[src]["last_value"] = (triggered_at, v)
        add_value(src, triggered_at, v)
        cleanup(src, triggered_at)
        _log(f"Stored {src} @ {triggered_at}: {v}")

    triggers[source] =state_trigger_var

def apply_threshold(src: str, v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    t = data[src]["threshold"]
    if t is None:
        return float(v)
    return 0.0 if v < t else float(v)



def cleanup(src: str, now: datetime) -> None:
    values = data[src]["values"]
    cutoff = now - timedelta(minutes=1) - BUFFER_MARGIN

    #_log(f"Cleaning up values for {src} before {cutoff}")
    #_log(f"====>> values before cleanup for {src}: {list(values)}")
    if not values:
        return

    arr = list(values)
    i = 0
    n = len(arr)
    while i < n and arr[i][0] < cutoff:
        i += 1

    if i == 0:
        return

    before = arr[:i]
    arr = arr[i:]

    if before:
        last_before = before[-1]
        if not arr or last_before[0] < arr[0][0]:
            arr.insert(0, last_before)

    values.clear()
    values.extend(arr)


def interpolate_linear(t0: datetime, t1: datetime, v0: float, v1: float, t: datetime) -> float:
    if t1 == t0:
        return v0
    ratio = (t - t0).total_seconds() / (t1 - t0).total_seconds()
    return v0 + (v1 - v0) * ratio


def interpolate_value(src: str, target_time: datetime) -> Optional[float]:
    values = data[src]["values"]
    last_value = data[src]["last_value"]
    mode_value = data[src]["mode"]

    linear = mode_value.startswith("linear")

    if not values:
        _log(f"interpolate_value: no values for {src}, using last_value")
        return apply_threshold(src, last_value[1]) if last_value else None

    before = [(t, v) for (t, v) in values if t <= target_time]
    after  = [(t, v) for (t, v) in values if t >= target_time]

    if before and after and before[-1][0] == after[0][0]:
        _log(f"interpolate_value: exact match for {src} at {target_time}")
        return apply_threshold(src, before[-1][1])

    if mode_value == "step":
        if before:
            return apply_threshold(src, before[-1][1])
        if after:
            return apply_threshold(src, after[0][1])
        return apply_threshold(src, last_value[1]) if last_value else None

    if before and after and before[-1][0] < target_time < after[0][0]:
        t0, v0 = before[-1]
        t1, v1 = after[0]
        return interpolate_linear(t0, t1, apply_threshold(src, v0), apply_threshold(src, v1), target_time)

    if not before and linear:
        if len(after) >= 2:
            t1, v1 = after[0]
            t2, v2 = after[1]
            return interpolate_linear(t1, t2, apply_threshold(src, v1), apply_threshold(src, v2), target_time)
        return apply_threshold(src, after[0][1])

    if not after and linear:
        if len(before) >= 2:
            t1, v1 = before[-2]
            t2, v2 = before[-1]
            return interpolate_linear(t1, t2, apply_threshold(src, v1), apply_threshold(src, v2), target_time)
        return apply_threshold(src, before[-1][1])

    return apply_threshold(src, last_value[1]) if last_value else None


def calc_time_weighted_avg_energy(src: str, start: datetime, end: datetime) -> tuple[Optional[float], Optional[float]]:
    values = data[src]["values"]
    last_value = data[src]["last_value"]

    if not values and not last_value:
        return None, None

    _log(f"Values: {src}")
    for (t1, v1) in values:
        _log(f"  Point: {t1}, {v1}")

    v_start = interpolate_value(src, start)
    v_end = interpolate_value(src, end)
    if v_start is None or v_end is None:
        return None, None

    pts = [(start, v_start)]

    for (t, v) in values:
        if start <= t <= end:
            pts.append((t, apply_threshold(src, v)))

    pts.append((end, v_end))

    _log(f"Calculating avg/energy for {src} from {start} to {end}")
    for (t1, v1) in pts:
        _log(f"  Point: {t1}, {v1}")



    total_power = 0.0
    total_energy_wh = 0.0
    total_time = 0.0

    for (t1, v1), (t2, _) in zip(pts[:-1], pts[1:]):
        dt = (t2 - t1).total_seconds()
        total_power += v1 * dt
        total_energy_wh += v1 * dt / 3600.0
        total_time += dt

    avg_power = round(total_power / total_time,2) if total_time > 0 else None
    total_energy_wh = round(total_energy_wh,2)
    return avg_power, total_energy_wh


def publish_result(target_avg: Optional[str], target_energy: Optional[str], avg_value: Optional[float], energy_value: Optional[float], data) -> None:
    num_values = len(data["values"])
    friendly_name = data["friendly_name"]
    unit_of_measurement = data["unit_of_measurement"]
    device_class = data["device_class"]

    if target_avg and avg_value is not None:
        state.set(target_avg, avg_value, {
            "unit_of_measurement": unit_of_measurement,
            "device_class": device_class,
            "samples": num_values,
            "friendly_name": (friendly_name+' avg min').title()
        })

    if target_energy and energy_value is not None:
        state.set(target_energy, energy_value, {
            "unit_of_measurement": "Wh",
            "samples": num_values,
            "friendly_name": (friendly_name+' energy').title()
        })



#########################
# MAIN CODE INITIALIZATION
#########################

def main_average_calculator():
    log.info("Running main processing function...")
    # ---- your main logic here ----

    for g in MAIN_SENSORS:
        main_sensor = g[0]
        group_info[main_sensor] = {
            "avg_suffix": g[1],
            "energy_suffix": g[2],
            "mode": g[3] if len(g) > 3 else DEFAULT_MODE,
            "threshold": g[4] if len(g) > 4 else DEFAULT_THRESHOLD
        }

        log.info(f"Setting up test group: {g}")
        log.info(f"  Group: {main_sensor}")


        attrs = state.getattr(main_sensor)
        log.warning(f"  Attributes: {attrs}")
        if ('entity_id' not in attrs) or (not attrs['entity_id']):
            log.warning(f"   {main_sensor} is not a group !")
            sensoren = [main_sensor]
        else:
            log.info(f"  {main_sensor} is GROUP with members: {attrs['entity_id']}")
            log.info(f"  Member count: {sensor.power_consumers.entity_id}")

            sensoren = attrs['entity_id']
            length = len(attrs['entity_id']) if ('entity_id' in attrs) and attrs['entity_id'] else 0
            groups.append(main_sensor)
            # for check whether a group member changed
            group_restored_list.append(main_sensor+'.restored')
            group_entity_list[main_sensor] = attrs['entity_id']


        for sensor in sensoren:
            set_data(main_sensor,sensor)
            sources.append(sensor)

        log.info(f"  Members: {groups}")
        log.info(f"  Member attributes: {group_restored_list}")
        log.info(f"  Sources: {sources}")

        for source in sources:
            state_trigger_factory(source)

        state_restored_factory(group_restored_list)

        log.info(f"Data initialized for group {triggers}")

#########################
# === TRIGGERS ===
#########################
@time_trigger("period(0, 1min)")
def periodic_update(trigger_type=None):
    now = datetime.now()
    log.info(f"Periodic update triggered at {now}")
    start = now - timedelta(minutes=1)

    for src in sources:
        data_src = data[src]
        target_avg_suffix= data_src["avg_suffix"]
        target_energy_suffix = data_src["energy_suffix"]
        target_avg = src + target_avg_suffix if target_avg_suffix else None
        target_energy = src + target_energy_suffix if target_energy_suffix else None

        cleanup(src, now)

        # Direct call, no async/await needed
        avg, energy = calc_time_weighted_avg_energy(src, start, now)
        data_src = data[src]

        publish_result(target_avg, target_energy, avg, energy, data_src)
        log.info(f"  Published results - Avg: {'{0:8.2f}'.format(avg)}, Energy: {'{0:8.2f}'.format(energy)}  for {src}")
        #now1 = datetime.now()
        #log.info(f"Periodic update triggered at {now1-now}")

    now1 = datetime.now()
    log.info(f"Periodic update ready at {now1-now}")

def state_restored_factory(group_restored_list):
    log.info(f"Creating group restored triggers for groups: {group_restored_list}")
    @state_trigger(group_restored_list)
    def group_members_changed(old_value=None, value=None,var_name=None):

        log.warning(f"Group members changed for {groups}: old_value={old_value}, new_value={value} var_name: {var_name}")
        log.info(f"Re-initializing group members and attributes. {group_restored_list}")

        attributes = state.getattr(var_name)
        entity_list = attributes['entity_id']
        log.info(f" Current entity_list: {entity_list}")

        set_old = set(group_entity_list[var_name])
        set_new = set(entity_list)

        added = set_new - set_old
        log.info(f"Added: {added}")

        removed= set_old - set_new
        log.info(f"Removes: {removed}")

        log.info(f"========>>> Old sources list: {sources}")
        if (removed):
            for s in removed:
                sources.remove(s)
                del data[s]
                del triggers[s]

            log.info(f" Removing sensors from sources list: {removed}")

        if (added):
            for s in added:
                sources.append(s)
                set_data(var_name,s)
                state_trigger_factory(s)

            log.info(f" Adding sensors to sources list: {added}")

        group_entity_list[var_name] = entity_list


        log.info(f"========>>> Updated sources list: {sources}")

event.fire("average_calculator_reloaded")

# End of module
