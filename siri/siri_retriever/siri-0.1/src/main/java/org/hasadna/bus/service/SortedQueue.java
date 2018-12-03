package org.hasadna.bus.service;

import static org.hasadna.bus.util.DateTimeUtils.DEFAULT_CLOCK;
import static org.hasadna.bus.util.DateTimeUtils.subtractMinutesStopAtMidnight;
import static org.hasadna.bus.util.DateTimeUtils.toDateTime;

import io.micrometer.core.instrument.Tags;
import io.micrometer.datadog.DatadogMeterRegistry;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

@Component
public class SortedQueue {

    protected final Logger logger = LoggerFactory.getLogger(SortedQueue.class);

    @Autowired
    private DatadogMeterRegistry registry;

    @PostConstruct
    public void init() {
        this.registry.gaugeCollectionSize("siri.scheduler.queue", Tags.empty(), this.queue);
    }

    public void put(Command command) {
        queue.offer(command);
    }

    public Command peek() {
        return queue.peek();
    }

    Command takeFromQueue() {
        return queue.poll();
    }

    // beware! this operation takes O(n) and is not accurate
    int size() {
        return queue.size();
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    public Command[] getAll() {
        Command data[] = new Command[queue.size()];
        Iterator<Command> iter = queue.iterator();
        int i = 0 ;
        while (iter.hasNext()) {
            data[i++] = iter.next();
        }
        return data;
    }

    public void delayNextExecution(final List<String> delayTillFirstDeparture) {
        if (CollectionUtils.isEmpty(delayTillFirstDeparture)) {
            return;
        }
        logger.info("delay querying {} routes: {}", delayTillFirstDeparture.size(), delayTillFirstDeparture);
        List<Command> candidatesForUpdatingNextExecution = new ArrayList<>();
        delayTillFirstDeparture.forEach(routeId ->
                candidatesForUpdatingNextExecution.addAll(removeByLineRef(routeId))
        );
        // now we change nextExecution in all of them
        List<Command> updatedNextExecution = new ArrayList<>();

        LocalDateTime currentTime = LocalDateTime.now(DEFAULT_CLOCK);
        DayOfWeek today = currentTime.getDayOfWeek();
        for (Command c : candidatesForUpdatingNextExecution) {
            try {
                if ((c.weeklyDepartureTimes != null) &&
                        c.weeklyDepartureTimes.containsKey(today) &&
                        !c.weeklyDepartureTimes.get(today).isEmpty()) {
                    String firstDeparture = c.weeklyDepartureTimes.get(today).get(0);
                    String evaluateAt = subtractMinutesStopAtMidnight(firstDeparture, 30);
                    c.nextExecution = toDateTime(evaluateAt);
                    c.isActive = false;
                    updatedNextExecution.add(c);
                }
            }
            catch (Exception ex) {
                logger.error("absorbing unhandled exception while calculating next execution of route " + c.lineRef, ex);
            }
        }

        int count = addBackToQueue(updatedNextExecution);

        if (count > 0) {
            logger.info("changed nextExecution for {} routes (postponed to 30 minutes before first departure)", count);
        }
        logger.info("currently {} active routes in the queue", showActive().size());
    }

    private int addBackToQueue(List<Command> updatedNextExecution) {
        int count = 0;
        for (Command c : updatedNextExecution) {
            try {
                boolean result = queue.offer(c);
                if (!result) {
                    logger.error("could not re-add to queue route id {}. To add it back you should call /schedules/read/all", c.lineRef);
                } else {
                    count = count + 1;
                }
            }
            catch (Exception ex) {
                logger.trace("absorbing exception during queue offer of route id {}. Check if it is in the Queue. If not, initiate re-read", c.lineRef);
                logger.trace("absorbing", ex);
            }
        }
        return count;
    }

    public void stopQueryingToday(final List<String> notNeededTillTomorrow) {
        if (CollectionUtils.isEmpty(notNeededTillTomorrow)) {
            return;
        }
        logger.info("stop querying {} routes: {}", notNeededTillTomorrow.size(), notNeededTillTomorrow);
        List<Command> candidatesForUpdatingNextExecution = new ArrayList<>();
        notNeededTillTomorrow.forEach(routeId ->
            candidatesForUpdatingNextExecution.addAll(removeByLineRef(routeId))
        );
        // now we change nextExecution in all of them
        List<Command> updatedNextExecution = new ArrayList<>();
        for (Command c : candidatesForUpdatingNextExecution) {
            if (!c.nextExecution.toLocalDate().isAfter(LocalDate.now(DEFAULT_CLOCK))) {
                c.nextExecution = LocalTime.of(23, 45).atDate(c.nextExecution.toLocalDate());
                c.isActive = false;
            }
            updatedNextExecution.add(c);
        }
        // add the back to the queue
        int count = addBackToQueue(updatedNextExecution);

        if (count > 0) {
            logger.info("changed nextExecution for {} routes (postponed to 23:45 or to 30 minutes after last arrival)", count);
        }
        logger.info("currently {} active routes in the queue", showActive().size());
    }

    public List<String> showAll() {
        return queue.stream().map(c -> c.toString()).collect(Collectors.toList());
    }

    // active schedules are schedules that were not re-scheduled to 23:45
    public List<String> showActive() {
        return queue.stream()
                .filter(c -> c.isActive)
                .map(c -> c.toString()).collect(Collectors.toList());
    }

    public List<Command> getAllSchedules() {
        return queue.stream().map(c -> c).collect(Collectors.toList());
    }


    List<Command> removeByLineRef(String lineRef) {
        logger.debug("removing route {} from scheduler queue (will be added again at midnight or whenever schedule file is re-read)", lineRef);
        List<Command> candidatesToRemoval = new ArrayList<>();
        Iterator iter = queue.iterator();
        while (iter.hasNext()) {
            Command current = (Command) iter.next();
            if (current.lineRef.equals(lineRef)) {
                candidatesToRemoval.add(current);
            }
        }
        List<Command> removed = new ArrayList<>();
        for (Command c : candidatesToRemoval) {
            boolean result = queue.remove(c);
            if (result) {
                removed.add(c);
            }
            else {
                logger.warn("removal of {} returned false", c);
            }
        }
//        logger.debug("return a list of {} schedules", removed.size());
//        logger.trace("return {}", removed);
        return removed;
    }

    /**
     * Beware! This operation will delete all schedulings from the queue!!
     * Are you sure this is what you intended?
     */
    public void removeAll() {
        // use removeIf with a predicate that always returns true
        queue.removeIf(c -> true);
    }

    private Queue<Command> queue = new PriorityBlockingQueue<>(1000, (c1, c2) -> c1.nextExecution.isBefore(c2.nextExecution)?-1:1);

}
