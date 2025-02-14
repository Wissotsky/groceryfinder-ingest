module CerberusFetchFiles
export fetchFiles


#= 
using DataFrames
using DataFramesMeta
using Inflate
using GZip
using ZipArchives
using CSV
using LibDeflate 
=#

using Dates
using HTTP
using EzXML
using JSON
using Serialization
using URIs


function is_less_for_cerberus_file_time(x,y)
    fileTimeStringX = x["ftime"]
    fileDateTimeX = DateTime(fileTimeStringX,dateformat"mm/dd/yyyy HH:MM")
    fileTimeStringY = y["ftime"]
    fileDateTimeY = DateTime(fileTimeStringY,dateformat"mm/dd/yyyy HH:MM")
    return fileDateTimeX < fileDateTimeY
end

function cerberus_auth_to_token(username::String,password::String)::String
    # get the CSRF token for logging in
    resp = HTTP.post("https://url.publishedprices.co.il/login/user"; query=Dict("r" => "", "username" => username, "password" => password, "Submit" => "Sign+in", "csrftoken" => ""))
    htmlstring = String(resp.body)
    htmlparsed = parsehtml(htmlstring)

    # <meta name="csrftoken" content="PpDWa7TP7DOwoBp62SlOYNLSv2s9t0rWOGcYcIcFA3k"/>&#13;
    node = findfirst("""//meta[@name='csrftoken']""",htmlparsed.root)
    csrfToken = node["content"]

    # get csrf token of signed up account
    resp = HTTP.post("https://url.publishedprices.co.il/login/user"; query=Dict("r" => "", "username" => username, "password" => password, "Submit" => "Sign+in", "csrftoken" => csrfToken))
    htmlstring = String(resp.body)
    htmlparsed = parsehtml(htmlstring)

    node = findfirst("""//meta[@name='csrftoken']""",htmlparsed.root)
    csrfToken = node["content"]
    return csrfToken
end

function fetchFiles(STORE_NAME::String, CERBERUS_PASSWORD::String, GLOBAL_FOLDER::String)
    # Authenticate with cerberus
    csrf_token = cerberus_auth_to_token(STORE_NAME,CERBERUS_PASSWORD)

    #
    # fetch pricefull file list
    resp = HTTP.post("https://url.publishedprices.co.il/file/json/dir"; query=Dict("iDisplayLength" => "100000", "sSearch" => "PriceFull", "csrftoken" => csrf_token))
    body = JSON.parse(String(resp.body))
    # sort by file recency. from most recent to least recent
    json_file_list_sorted_by_recency = sort(body["aaData"],lt=is_less_for_cerberus_file_time,rev=true)
    # filter out files with NULL in their names
    filter!(x -> !occursin("NULL",x["name"]),json_file_list_sorted_by_recency)

    pricefull_file_links_list = map(x -> "https://url.publishedprices.co.il/file/d/$(x["name"])?dm=0",json_file_list_sorted_by_recency)

    #
    # fetch promofull file list
    resp = HTTP.post("https://url.publishedprices.co.il/file/json/dir"; query=Dict("iDisplayLength" => "100000", "sSearch" => "PromoFull", "csrftoken" => csrf_token))
    body = JSON.parse(String(resp.body))
    # sort by file recency. from most recent to least recent
    json_file_list_sorted_by_recency = sort(body["aaData"],lt=is_less_for_cerberus_file_time,rev=true)
    # filter out files with NULL in their names
    filter!(x -> !occursin("NULL",x["name"]),json_file_list_sorted_by_recency)

    promofull_file_links_list = map(x -> "https://url.publishedprices.co.il/file/d/$(x["name"])?dm=0",json_file_list_sorted_by_recency)

    # get cookie from response
    cookies = HTTP.Cookies.cookies(resp)
    cftpsid_cookie = HTTP.Cookies.stringify(cookies[1])

    file_links_list = vcat(pricefull_file_links_list,promofull_file_links_list)

    # load StoresDataframe from serialized
    StoresDataframe = deserialize("$GLOBAL_FOLDER/$STORE_NAME-StoresTable.df")

    chainIdLength = 13 # TODO: hardcoded

    # list of store ids that we want to fetch
    allowed_store_ids = [div(storeId,10^chainIdLength) for storeId in StoresDataframe.StoreIds]

    # list of pricefull and promofull files
    filter!(x -> occursin(r"PriceFull",x) || occursin(r"PromoFull",x),file_links_list)

    # list of store urls that we want to fetch
    filter!(x -> in(parse(Int64,match(r"-(\d*)-",x).captures[1]),allowed_store_ids),file_links_list)

    # split by pricefull and promofull
    pricefull_file_links_list = filter(x -> occursin(r"PriceFull",x),file_links_list)
    promofull_file_links_list = filter(x -> occursin(r"PromoFull",x),file_links_list)
    # remove duplicates by storeid
    unique!(x -> parse(Int64,match(r"-(\d*)-",x).captures[1]),pricefull_file_links_list)
    unique!(x -> parse(Int64,match(r"-(\d*)-",x).captures[1]),promofull_file_links_list)

    # add file_links_list together
    file_links_list = vcat(pricefull_file_links_list,promofull_file_links_list)

    output_folder_name = "$STORE_NAME-output"
    # write to txt file
    open("urls.txt", "w") do io
        for url in file_links_list
            println(io, url)
            println(io, "   header= Cookie:$cftpsid_cookie")
            println(io, "   out=$GLOBAL_FOLDER/$output_folder_name/$(URIs.splitpath(parse(URI, url))[3])")
        end
    end
    # julia help run(), run with wait=true

    run(`aria2c --input-file urls.txt`)

end

end