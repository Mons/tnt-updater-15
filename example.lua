local yesterday = box.pack('l',math.floor( os.time() / 86400 - 1 ) * 86400 - 3600*3);

updater('db-update-name',{
	space = 1
	index = 0,
	waitevery = 5000,
	takeby    = 500,
	fiberpool = 50,
	keyfields = function(v) return v[1],v[2] end,
	picker = function(v)
		return #v < 8
	end,
	updater = function(t)
		if #t == 5 then
			box.update(1,{t[1],t[2]},"=p=p=p",
				5, '',
				6, box.pack('i',0),
				7, yesterday
			)
		elseif #t == 6 then
			box.update(1,{t[1],t[2]},"=p=p",
				5, '',
				7, yesterday
			)
		elseif #t == 7 then
			box.update(1,{t[1],t[2]},"=p=p",
				5, '',
				7, yesterday
			)
		else
			print("Strange tuple for update: ",t);
		end
	end
});
