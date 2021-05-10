# List of APIs for the dashboard

## `/overall`
### Input
- `client_id`
- `time_from`
- `time_to`

### Output
Object with the following attributes:
- `sessions`
- `mins/session`
- `perc_first_time_sessions`

## `/trends`
### Input
- `client_id`
- `time_from`
- `time_to`
- `referrer` (optional)
- `device_type` (optional)
- `country` (optional)
- `city` (optional)
- `url` (optional)

### Output
Object with the following attributes:
- `pvs_7d_ago`: `[ [epoch_time, pvs], ... ]`
- `total_pvs`: `[ [epoch_time, pvs], ... ]`
- `search_pvs`: `[ [epoch_time, pvs], ... ]`
- `social_pvs`: `[ [epoch_time, pvs], ... ]`
- `direct_pvs`: `[ [epoch_time, pvs], ... ]`
- `forum_pvs`: `[ [epoch_time, pvs], ... ]`
- `other_pvs`: `[ [epoch_time, pvs], ... ]`

## `/urls`
### Input
- `client_id`
- `time_from`
- `time_to`
- `referrer` (optional)
- `device_type` (optional)
- `country` (optional)
- `city` (optional)
- `url` (optional)

### Output
Array of objects, each of which have the following attributes:
- `page_path`
- `pvs`
- `avg_seconds`
- `aff_idx`
- `new_users_perc`

## `/macro_agg`
### Input
- `client_id`
- `time_from`
- `time_to`
- `group_by`
- `val_to_agg`
- `referrer` (optional)
- `device_type` (optional)
- `country` (optional)
- `city` (optional)
- `url` (optional)

### Output
Array of objects, each of which have the following attributes:
- `group_by_key`
- `agg_value`

## `/geo`
### Input
- `client_id`
- `time_from`
- `time_to`
- `referrer` (optional)
- `device_type` (optional)
- `country` (optional)
- `city` (optional)
- `url` (optional)

### Output
Array of objects, each of which have the following attributes:
- `country`
- `city`
- `pvs`

## `/scroll_depth`
### Input
- `client_id`
- `time_from`
- `time_to`
- `url`
- `referrer` (optional)
- `device_type` (optional)
- `country` (optional)
- `city` (optional)

### Output
Array of numbers in order

## `/timespent`
### Input
- `client_id`
- `time_from`
- `time_to`
- `url`
- `referrer` (optional)
- `device_type` (optional)
- `country` (optional)
- `city` (optional)

### Output
Array of numbers in order

## `/read_next`
### Input
- `client_id`
- `time_from`
- `time_to`
- `url`
- `referrer` (optional)
- `device_type` (optional)
- `country` (optional)
- `city` (optional)

### Output
Array of objects, each of which has the following attributes:
- `url`
- `pvs`

## `/event_list`
### Input
- `client_id`
- `time_from`
- `time_to`
- `url`
- `referrer` (optional)
- `device_type` (optional)
- `country` (optional)
- `city` (optional)

### Output
Array of objects, each of which has the following attributes:
- `category`
- `event_name`
- `counts`