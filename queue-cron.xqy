xquery version "1.0-ml";

import module namespace q = "http://grtjn.nl/marklogic/queue" at "queue-lib.xqy";

declare option xdmp:mapping "false";

declare variable $id external;

xdmp:log(fn:concat("Queue cron ", $id, ": beginning..")),

(: wait 5 sec to allow parent thread to finish :)

xdmp:sleep(5000),

if (q:should-cron-stop()) then
	xdmp:log(fn:concat("Queue cron ", $id, ": stopping.."))
else if (q:is-cron-active()) then
	xdmp:log(fn:concat("Queue cron ", $id, ": already active.."))
else
	let $claim-lock := q:claim-cron-lock()
	let $nr-threads-available := q:get-task-server-threads-available()
	let $next-tasks := q:get-tasks(1, $nr-threads-available)
	let $do-tasks :=
		if ($nr-threads-available gt 0) then
			if (count($next-tasks) gt 0) then
				for $task in $next-tasks
				let $task-id := $task/q:task/@id
				let $task-created := $task/q:task/@created
				let $task-module := $task/q:task/q:module
				let $task-prio := $task/q:task/q:prio
				return (
					q:exec-task($task-id),
					xdmp:log(fn:concat("Queue cron ", $id, ": launched task ", $task-id, " ", $task-created, " ", $task-module, " ", $task-prio, ".."))
				)
			else
				xdmp:log(fn:concat("Queue cron ", $id, ": nothing to do.."))
		else
			xdmp:log(fn:concat("Queue cron ", $id, ": task server busy.."))
	return (
		xdmp:log(fn:concat("Queue cron ", $id, ": sleeping ", $q:cron-sleep, " msec..")),
		xdmp:sleep($q:cron-sleep),
		q:release-cron-lock(),
		xdmp:spawn("queue-cron.xqy", (xs:QName("id"), $id))
	)
			
