#ifndef TIBER_COND_UTILS_H
#define TIBER_COND_UTILS_H

#include<stdint.h>
#include<tiber/tiber.h>
#include<errno.h> // only for ETIMEDOUT

#include<posixutils/pthread_cond_utils.h> // only for BLOCKING and NON_BLOCKING macros
#include<posixutils/timespec_utils.h>

// below functions can be used to performed a timedwait on a condition variable for a duration given as its last parameter
// please be sure that duration will be updated to the time remaining from the provided duration, so you can wait on the same remaining duration in your next call
static inline int tiber_cond_timedwait_for_timespec(tiber_cond *restrict cond, tiber_mutex *restrict mutex, struct timespec* duration);
static inline int tiber_cond_timedwait_for_seconds(tiber_cond *restrict cond, tiber_mutex *restrict mutex, uint64_t* duration_seconds);
static inline int tiber_cond_timedwait_for_milliseconds(tiber_cond *restrict cond, tiber_mutex *restrict mutex, uint64_t* duration_milliseconds);
static inline int tiber_cond_timedwait_for_microseconds(tiber_cond *restrict cond, tiber_mutex *restrict mutex, uint64_t* duration_microseconds);
static inline int tiber_cond_timedwait_for_nanoseconds(tiber_cond *restrict cond, tiber_mutex *restrict mutex, uint64_t* duration_nanoseconds);

int tiber_cond_timedwait_for_timespec(tiber_cond *restrict cond, tiber_mutex *restrict mutex, struct timespec* duration)
{
	// get time before waiting
	struct timespec before_wait;
	clock_gettime(CLOCK_MONOTONIC, &before_wait);

	int result;
	{
		const struct timespec wait_until = timespec_add(before_wait, *duration);
		result = tiber_cond_timedwait(cond, mutex, &wait_until);
	}

	// get time after the wait completes
	struct timespec after_wait;
	clock_gettime(CLOCK_MONOTONIC, &after_wait);

	// calculate time we waited for
	// we are sure that after_wait > before_wait, as we are using CLOCK_MONOTONIC
	const struct timespec waited_for = timespec_sub(after_wait, before_wait);

	// remove the duration that we waited for from the duration
	if(timespec_compare(waited_for, *duration) > 0) // if we for some reason waited for more than the duration
		(*duration) = (struct timespec){};
	else
		(*duration) = timespec_sub(*duration, waited_for);

	return result;
}

// you may use all possible uint64_t values for the above tiber_cond_timedwait_for_*seconds() functions
// but there are special values you need to take care of given below, this gives immense flexibility to wait NON_BLOCKINGly, BLOCKINGly or with a fixed positive timeout duration
#define NON_BLOCKING   UINT64_C(0)
#define BLOCKING       UINT64_MAX

#define tiber_cond_timedwait_for_(unit)                                                                                                                    \
static inline int tiber_cond_timedwait_for_ ## unit (tiber_cond *restrict cond, tiber_mutex *restrict mutex, uint64_t* duration_ ## unit)                  \
{                                                                                                                                                          \
	if((*duration_ ## unit) == NON_BLOCKING) /* if it was suppossed to be a NON_BLOCKING call then timeout immediately */                                  \
		return ETIMEDOUT;                                                                                                                                  \
                                                                                                                                                           \
	if((*duration_ ## unit) == BLOCKING) /* if it was suppossed to be a BLOCKING call then use the non-timeout version to wait */                          \
		return tiber_cond_wait(cond, mutex);                                                                                                               \
                                                                                                                                                           \
	struct timespec duration = timespec_from_ ## unit(*duration_ ## unit);                                                                                 \
                                                                                                                                                           \
	int result = tiber_cond_timedwait_for_timespec(cond, mutex, &duration);                                                                                \
                                                                                                                                                           \
	(*duration_ ## unit) = timespec_to_ ## unit(duration);                                                                                                 \
                                                                                                                                                           \
	return result;                                                                                                                                         \
}                                                                                                                                                          \
// new line break here

tiber_cond_timedwait_for_(seconds)
tiber_cond_timedwait_for_(milliseconds)
tiber_cond_timedwait_for_(microseconds)
tiber_cond_timedwait_for_(nanoseconds)

#endif