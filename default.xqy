xquery version "1.0-ml";

import module namespace q = "http://grtjn.nl/marklogic/queue" at "queue-lib.xqy";

declare namespace ss = "http://marklogic.com/xdmp/status/server";
declare namespace hs = "http://marklogic.com/xdmp/status/host";

declare option xdmp:mapping "false";

xdmp:set-response-content-type("text/html"),

let $action := (xdmp:get-request-field("action")[. != ''], "show")[1]
let $id := (xdmp:get-request-field("id")[. != ''], string(xdmp:random()))[1]
let $module := xdmp:get-request-field("module", "")
let $prio as xs:integer := xs:integer((xdmp:get-request-field("prio")[. != ''], "0")[1])
let $params as map:map := map:map()
let $start := max((xs:integer((xdmp:get-request-field("start")[. != ''], "1")[1]), 1))
let $page-size := xs:integer((xdmp:get-request-field("page")[. != ''], "50")[1])
let $msg := xdmp:get-request-field("msg", "")
let $auto-refresh as xs:boolean := fn:boolean(fn:lower-case(xdmp:get-request-field("auto-refresh", "false")) = ('1', 'yes', 'true'))

return

if ($action ne "show") then
	let $msg :=
		try {
			xdmp:invoke("do-queue-action.xqy",
				(xs:QName("action"), $action,
				 xs:QName("id"), $id,
				 xs:QName("module"), $module,
				 xs:QName("prio"), $prio,
				 xs:QName("params"), $params)
			)
		} catch ($e) {
			string($e)
		}
	let $redirect-uri :=
		fn:concat(fn:replace(q:get-request-url(), '\?.*$', ''), "?action=show&amp;msg=", $msg, "&amp;start=", $start, "&amp;page-size=", $page-size)
	return
	
	(: Annoyingly, using xdmp:redirect-uri() causes updates NOT to be committed, so we use meta refresh instead.. :)
	
	<html>
	<head>
		<meta http-equiv="refresh" content="{if ($action = ('start-cron')) then 10 else 0}; url={$redirect-uri}"/>
	</head>
	<body>
		<h1>Task queue manager</h1>
		<font color="red">One moment please..</font>
		<a href="{$redirect-uri}">back to manager</a>
	</body>
	</html>
	
else
	let $host-id as xs:unsignedLong := xdmp:host()
	let $host-status := xdmp:host-status($host-id)
	let $task-server-id as xs:unsignedLong := $host-status//hs:task-server-id
	let $task-server-status := xdmp:server-status($host-id, $task-server-id)
	let $task-server-threads as xs:integer := $task-server-status//ss:threads
	let $task-server-max-threads as xs:integer := $task-server-status//ss:max-threads
	return

	<html>
	{ if ($auto-refresh) then
	<head>
		<meta http-equiv="refresh" content="5; url=?auto-refresh=true&amp;start={$start}&amp;page={$page-size}"/>
	</head>
	else ()
	}
	<body>
		<h1>Task queue manager</h1>
		<div>
			{
			if ($auto-refresh) then
				<a href="?start={$start}&amp;page={$page-size}">stop refresh</a>
			else
				<a href="?auto-refresh=true&amp;start={$start}&amp;page={$page-size}">auto refresh</a>
			}
			<a href="?action=flush-task-server&amp;start={$start}&amp;page={$page-size}">flush-task-server</a>
			<a href="?start={$start}&amp;page={$page-size}">refresh</a>
		</div>
		<div><font color="red">{$msg}&#160;</font></div>
		
		<h2>Task server</h2>
		<div>Status at: {fn:current-dateTime()}</div>
		<div>Max Threads: {$task-server-max-threads}</div>
		<div>Used Threads: {$task-server-threads}</div>
		<div>Available Threads: {q:get-task-server-threads-available()}</div>
		<div>Queued Tasks: {q:get-queued-tasks-count()}</div>
		
		<h2>Create task</h2>
		<form action="?">
			Module: <input type="text" name="module" value="{$module}" /><br/>
			Priority: <input type="text" name="prio" value="{$prio}" /> ({$q:min-prio} upto {$q:max-prio})<br/>
			<input type="hidden" name="action" value="create" />
			<input type="submit" value="Create"/>
		</form>
		
		<h2>Cron</h2>
		{
		if (q:is-cron-active()) then
			<div>Cron is <font color="green">active</font> (<a href="?action=stop-cron&amp;start={$start}&amp;page={$page-size}">stop cron</a>)</div>
		else
			<div>Cron is <font color="red">inactive</font> (<a href="?action=start-cron&amp;start={$start}&amp;page={$page-size}">start cron</a>)</div>
		}
		
		<h2>Current queue</h2>
		<table>
			<tr>
				<th>Nr</th><th>Id</th><th>Prio</th><th>Created</th><th>Module</th><th>Actions</th>
			</tr>
			{
				for $task at $pos in q:get-tasks($start, $page-size)
				let $pos := $pos + $start - 1
				let $id := data($task/q:task/@id)
				let $created := data($task/q:task/@created)
				let $module := data($task/q:task/q:module)
				let $prio := data($task/q:task/q:prio)
				return
					<tr>
						<td>{$pos}</td><td><a href="view-task.xqy?id={$id}" target="_blank">{$id}</a></td><td>{$prio}</td><td>{$created}</td><td>{$module}</td><td>
							<a href="?start={$start}&amp;page={$page-size}&amp;action=remove&amp;id={$id}&amp;module={$module}&amp;prio={$prio}">X</a>
							<a href="?start={$start}&amp;page={$page-size}&amp;action=incprio&amp;id={$id}&amp;module={$module}&amp;prio={$prio}">+</a>
							<a href="?start={$start}&amp;page={$page-size}&amp;action=decprio&amp;id={$id}&amp;module={$module}&amp;prio={$prio}">-</a>
							<a href="?start={$start}&amp;page={$page-size}&amp;action=exec&amp;id={$id}&amp;module={$module}&amp;prio={$prio}">&gt;&gt;</a>
						</td>
					</tr>
			}
		</table>
		<a href="?start={$start - $page-size}&amp;page={$page-size}">prev</a>
		<a href="?start={$start + $page-size}&amp;page={$page-size}">next</a>
	</body>
	</html>

