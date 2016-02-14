require( "EditDistance" )

function SubField( String, fields )
    for i, j in pairs( fields ) do
        local subStr = "%$%(" .. i ..  "%)" 
        String =  String:gsub( subStr, j )
    end
    return String
end

function SqlHelper( params )
    local res = nil
    local ok = true
    local first = true
    local stmt = params.db:prepare( params.sql )
    ok = ok and stmt
    for i, j in ipairs( params.param ) do
        ok = ok and (stmt:bind(i, j ) == sqlite3.OK )
    end
    if ok then 
        for res1 in stmt:nrows() do
            local resRow
            if type( params.result ) == "table" then
                resRow = {}
                for k,v in ipairs( params.result ) do
                    resRow[v] = res1[ v ]
                end
            else
                resRow = res1[ params.result]
            end
            if res == nil then 
               res = resRow
            else
               if first then
                  local tmp = res
                  res = {}
                  res[1] = tmp
                  first = false
               end
               res[ #res + 1] = resRow
            end
        end
    end
    return res
end



function JaccardApprox( db, ids, tble, field )
    local template = "SELECT MIN( count )as _min, MAX(count) as _max FROM $(table)_Size WHERE id IN ( ?,? );"
    local sql = template:gsub( "%$%(table%)", tble )
    local min_max = SqlHelper{ db = db, sql = sql, param = ids, result = { "_min", "_max" } }
    local result = nil
    if min_max then
       if min_max._max == 0 then
           return 0; -- no records, no difference
       end
       result = min_max._min / min_max._max
    end
    result = result or 0

    return 1 - result

end
function JaccardExact( db, ids, tble, field )
    local template = "SELECT count(*) AS Result FROM (SELECT $(Field) FROM $(Table) WHERE id = ? $(SET_TYPE) SELECT $(Field) FROM $(Table) WHERE id = ? );"
    local t1 = template:gsub( "%$%(Table%)", tble )
    local t2 = t1:gsub( "%$%(Field%)", field )
    local sql1 = t2:gsub( "%$%(SET_TYPE%)", "INTERSECT" )
    local sql2 = t2:gsub( "%$%(SET_TYPE%)", "UNION" )
    local n = SqlHelper{ db = db, sql = sql1, param = ids, result = "Result" }
    local d = SqlHelper{ db = db, sql = sql2, param = ids, result = "Result" }
    if n == 0 and d == 0 then return 0 end -- no data for both, return same.
    if d == 0 or d == nil then return 1 end
    if n == nil then n = 0 end
    return 1 - n/d
end

function EditDistanceExact( db, ids, tble, field )
    if ids[1] == ids[2] then
       return 0
    end

    local template = "SELECT $(Field) as Result FROM $(Table) WHERE id IN( ?,?);"
    local t1 = template:gsub( "%$%(Table%)", tble )
    local sql= t1:gsub( "%$%(Field%)", field )
    local strs = SqlHelper{ db = db, sql = sql, param = ids, result = "Result" }
--    local str2 = SqlHelper{ db = db, sql = sql, param = {ids[2]}, result = "Result" }
    local str1 = strs[1]
    local str2 = strs[2]

    local dist = EditDistance.Distance( str1, str2 ) / 100
    return dist
end
function MakeEditDistance( DBtable, field, scale )
    return {
       Approx = function( db, ids )
           return EditDistanceExact( db, ids, DBtable, field ) * scale
       end,
       Exact = function( db, ids )
           return EditDistanceExact( db, ids, DBtable, field ) * scale
       end,
    }
end

function MakeJaccardDistance( DBtable, field, scale )
    return { 
        Approx = function( db, ids )
            return JaccardApprox( db, ids, DBtable, field ) * scale
        end,
        Exact = function( db, ids )
            return JaccardExact( db, ids, DBtable, field ) * scale
        end,
        Size = scale,
    }
end

function CalcDistance( db, ids, distances, exact )
    local total = 0
    for i, j in pairs( distances ) do
        if exact then
            total = total + j.Exact( db, ids )
        else
            total = total + j.Approx( db, ids )
        end
    end
    return total
end
