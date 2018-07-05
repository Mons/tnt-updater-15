function updater(name,opts)
	local time
	if box.time~=nil then time=box.time else time=os.time end
	
	local o = {}
	if _G[name] then
		o.fiber = _G[name].fiber
	end
	
	o.waitevery  = opts.waitevery  or 1000
	-- o.printevery = opts.printevery or 1000
	o.takeby     = opts.takeby     or 500
	o.fiberpool  = opts.fiberpool -- or 10
	o.keyfields  = opts.keyfields
	o.updater    = opts.updater
	o.picker     = opts.picker
	
	assert(type(o.keyfields) == 'function', "Need .keyfield funtion")
	assert(type(o.updater) == 'function', "Need .updater funtion")
	
	if o.waitevery > 1000 then
		print("\n\n\n!!! waitevery option is too big: ",o.waitevery,"\n!!! better  use  something like  1000\n\n\n")
	end
	
	assert(opts.space, "Required option .space")
	assert(opts.index, "Required option .index")
	local sp = box.space[ opts.space ]
	assert(sp, "Space "..opts.space.." is unknown")
	local i = sp.index[opts.index]
	assert(i, "Index "..opts.index.." in space "..opts.space.." is unknown")
	if i.type ~= "TREE" and i.type ~= "AVLTREE" then
		error("Index "..opts.index.." in space "..opts.space.." is non-iteratable",2)
	end
	
	local PREF = ''
	
	o.stop = function()
		if o.fiber then
			local r,id = pcall(function() return o.fiber:id() end)
			if r then
				print("Stopping fiber ",id)
				box.fiber.cancel(o.fiber)
				o.fiber = nil
			else
				print("Can't cancel old fiber: ", id)
				--print("If it's really dead, clean state with `lua "..name..".fiber = nil'")
				o.fiber = nil
			end
		else
			print("Not running")
		end
	end
	
	o.run = function (debug, detach, verbose)
		if o.fiber then
			error("Script already running, stop it manually",1)
		end
		if detach == nil then detach = true end
		if debug  == nil then debug  = true end
		if verbose and not o.verbosity then
			o.verbosity = function(v)
				print(" take ",v);
			end
		end
		if not o.verbosity then verbose = false end
		print("start with debug = ",debug)
		
		local fclock = os.clock;
		
		local size = sp:len()
		local start = time()
		local prev  = start
		if not o.printevery or o.printevery > size/2 then
			o.printevery = math.floor(size/50)
		end
		
		local function runit()
			if (detach) then
				box.fiber.detach()
			end
			box.fiber.name(name)
			print(string.format("Processing %d items; fpool: %s; wait: 1/%d; take: %d",size, o.fiberpool or 'false', o.waitevery, o.takeby));
			local it = i:iterator( box.index.ALL )
			local v
			local toupdate = {}
			local working = true
			local c = 0
			local u = 0
			local csw   = 0
			local clock = 0
			local clock1 = fclock()
			while working do c = c + 1
				-- if c >= 1e6 then break end
				if c % o.waitevery == 0 then
					
					clock = clock + ( fclock() - clock1 )
					csw = csw + 1
					
					box.fiber.sleep( 0 )
					
					clock1 = fclock()
					
					-- reposition iterator after sleep to previous element
					-- beware not to enter here on first step, since we have no v
					it = i:iterator( box.index.GT, o.keyfields(v) )
				end
				v = it()
				if v == nil then break end
				
				if not o.picker or o.picker(v) then
					u = u + 1
					if verbose then
						o.verbosity( v );
					end
					table.insert(toupdate, v)
				end
				
				if #toupdate >= o.takeby then
					clock = clock + ( fclock() - clock1 )
					csw = csw + 1
					if not debug then
						if o.fiberpool then
							local ix = 0
							local cv = 0
							local ch = box.ipc.channel()
							for k = 1,3 do
								cv = cv + 1
								-- print("wrap, ",cv)
								box.fiber.wrap(function()
									pcall(function()
									box.fiber.name(name..".fp."..tostring(k))
									while working do
										ix = ix + 1
										if ix > #toupdate then
											--print("ix = ",ix," break ",cv)
											break
										else
											--print("ix = ",ix," work ",cv)
										end
										local v = toupdate[ix]
										local r,e = pcall(o.updater, v)
										if not r then
											local t = tostring(v)
											if #t > 1000 then t = string.sub(t,1,995)..'...' end
											print("failed to update ",t,": ",e)
											if o.break_on_die then
												working = false
												break
											end
										end
									end
									end)
									cv = cv - 1
									--print("cv = ",cv)
									if cv == 0 then ch:put(true,0) end
								end)
							end
							local _ = ch:get()
							ch:close()
							--print("unlock")
						else
							for _,v in ipairs(toupdate) do
								local r,e = pcall(o.updater, v)
								if not r then
									local t = tostring(v)
									if #t > 1000 then t = string.sub(t,1,995)..'...' end
									print("failed to update ",t,": ",e)
									if o.break_on_die then
										working = false
										break
									end
								end
							end
						end
					end
					clock1 = fclock()
					toupdate = {}
					it = i:iterator(box.index.GT, o.keyfields(v))
				end
				
				if c % o.printevery == 0 then
					local now = time()
					local r,e = pcall(function()
						local run = now - start
						local run1 = now - prev
						local rps = c/run
						local rps1 = o.printevery/run1
						collectgarbage("collect")
						local mem = collectgarbage("count")
						print(string.format("%sProcessed %d (%d) (%0.1f%%) in %0.3fs (rps: %.0f tot; %.0f/%.1fs; %.2fms/c) ETA:+%ds (or %ds) Mem: %dK",
							PREF,
							c, u,
							100*c/size,
							run,
							c/run, rps1, run1,
							1000*clock/csw,
							
							(size - c)/rps1,
							(size - c)/rps,
							
							mem
						))
						--print("Processed ",c, " (",pf(100*c/size,10),"%) in ", pf(run,1E3), " seconds (rps: ", math.floor( c/run ),"/",math.floor( printevery/run1 ), ") ETA: ", pf(size/rps-run,1E3), "s", " Mem: ",math.floor(mem),"K" )
					end)
					if not r then print(e) end
					prev = now
				end
			end
			if working then
				if #toupdate > 0 then
					if not debug then
						for _,v in ipairs(toupdate) do
							local r,e = pcall(o.updater, v)
							if not r then
								local t = tostring(v)
								if #t > 1000 then t = string.sub(t,1,995)..'...' end
								print("failed to update ",t,": ",e)
								if o.break_on_die then
									working = false
									break
								end
							end
						end
					end
				end
			end
		
			o.fiber = nil
			local run = time()-start
			print(string.format("%sProcessed %d in %0.3f seconds (rps: %0.1f/s; up: %0.1f/s) CPU: %0.4fs, %0.4fms/call",PREF,c-1, run, c/run, u/run, clock, 1000*clock/csw ))
			print(string.format("%sBefore: %d; after: %d. Selected: %d; updated: %d%s",PREF, size, sp:len(), c-1, u, debug and " (not really)" or ""))
		end
		
		if (detach) then
			local fiber = box.fiber.create(runit)
			box.fiber.resume(fiber)
			o.fiber = fiber
			print("created background fiber: ",box.fiber.id(fiber));
			print("can be cancelled with `lua "..name..".stop()' or `lua box.fiber.cancel(box.fiber.find("..box.fiber.id(fiber) .. "))`")
		else
			o.fiber = box.fiber.self()
			runit()
		end
	end
	
	_G[name] = o
	
	print("Run me as `lua "..name..".run(debug = true [ ,detach = true, [ ,verbose = false ] ] )'")
	print("\t: debug: work readonly")
	print("\t: detach: go in background (recommended)")
	print("\t: verbose: call verbosity func for every row")
end

return updater
