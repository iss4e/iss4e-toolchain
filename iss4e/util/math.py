from datetime import timedelta


def differentiate(samples, label, label_diff=None, delta_time=timedelta(seconds=1), attr_time='Stamp'):
    if not label_diff:
        label_diff = label + '_diff'

    last_sample = None
    for sample in samples:
        if last_sample is None or last_sample[label] is None or sample[label] is None:
            sample[label_diff] = 0
        else:
            sample[label_diff] = (sample[label] - last_sample[label]) / \
                                 ((sample[attr_time] - last_sample[attr_time]) / delta_time)
        yield sample
        last_sample = sample


def smooth(samples, label, label_smooth=None, alpha=.95, default_value=None, is_valid=None):
    """Smooth values using the formula
    `samples[n][label_smooth] = alpha * samples[n-1][label_smooth] + (1 - alpha) * samples[n][label]`

    If a value isn't available, or `is_valid(sample, last_sample, label)` returns false,
     the previous smoothed value is used.
    If none of these exist, default_value is used. If default_value is callable,
     `default_value(sample, last_sample, label)` will be called
    """
    if not label_smooth:
        label_smooth = label + '_smooth'

    last_sample = None
    for sample in samples:
        yield smooth1(sample, last_sample, label, label_smooth, alpha, default_value, is_valid)
        last_sample = sample


def smooth1(sample, last_sample, label, label_smooth=None, alpha=.95, default_value=None, is_valid=None):
    if not label_smooth:
        label_smooth = label + '_smooth'

    if not (sample and label in sample and sample[label] is not None) or \
            (callable(is_valid) and not is_valid(sample, last_sample, label)):
        if callable(default_value):
            sample[label_smooth] = default_value(sample, last_sample, label, label_smooth)
        else:
            sample[label_smooth] = default_value
    else:
        if not (last_sample and label_smooth in last_sample and last_sample[label_smooth] is not None):
            # 1nd sensible value in the list, use it as starting point for the smoothing
            sample[label_smooth] = sample[label]
        else:
            # current and previous value available, apply the smoothing function
            sample[label_smooth] = alpha * last_sample[label_smooth] \
                                   + (1 - alpha) * sample[label]
    return sample


def smooth_ignore_missing(sample, last_sample, label, label_smooth):
    if last_sample:
        if label_smooth in last_sample:
            return last_sample[label_smooth]
        elif label in last_sample:
            return last_sample[label]

    return None


def smooth_reset_stale(max_sample_delay):
    return lambda sample, last_sample, label: last_sample and last_sample['Stamp'] - sample['Stamp'] < max_sample_delay
