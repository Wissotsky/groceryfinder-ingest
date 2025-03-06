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
    if fileTimeStringX == "" fileTimeStringX = "01/01/1970 00:00" end
    fileDateTimeX = DateTime(fileTimeStringX,dateformat"mm/dd/yyyy HH:MM")
    fileTimeStringY = y["ftime"]
    if fileTimeStringY == "" fileTimeStringY = "01/01/1970 00:00" end
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

function fetchFiles(STORE_NAME::String, CERBERUS_PASSWORD::String, GLOBAL_FOLDER::String,path_mapping_dict::Dict)
    # Authenticate with cerberus
    csrf_token = cerberus_auth_to_token(STORE_NAME,CERBERUS_PASSWORD)

    if STORE_NAME == "yuda_ho"
        # fetch pricefull file list
        resp = HTTP.post("https://url.publishedprices.co.il/file/json/dir"; query=Dict("iDisplayLength" => "100000", "sSearch" => "PriceFull", "csrftoken" => csrf_token, "cd" => "/Yuda"))
        body = JSON.parse(String(resp.body))
        # sort by file recency. from most recent to least recent
        json_file_list_sorted_by_recency = sort(body["aaData"],lt=is_less_for_cerberus_file_time,rev=true)
        # filter out files with NULL in their names
        filter!(x -> !occursin("NULL",x["name"]),json_file_list_sorted_by_recency)

        pricefull_file_links_list = map(x -> "https://url.publishedprices.co.il/file/d/Yuda/$(x["name"])?dm=0",json_file_list_sorted_by_recency)

        #
        # fetch promofull file list
        resp = HTTP.post("https://url.publishedprices.co.il/file/json/dir"; query=Dict("iDisplayLength" => "100000", "sSearch" => "PromoFull", "csrftoken" => csrf_token , "cd" => "/Yuda"))
        body = JSON.parse(String(resp.body))
        # sort by file recency. from most recent to least recent
        json_file_list_sorted_by_recency = sort(body["aaData"],lt=is_less_for_cerberus_file_time,rev=true)
        # filter out files with NULL in their names
        filter!(x -> !occursin("NULL",x["name"]),json_file_list_sorted_by_recency)

        promofull_file_links_list = map(x -> "https://url.publishedprices.co.il/file/d/Yuda/$(x["name"])?dm=0",json_file_list_sorted_by_recency)
    else
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
    end

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
    mkpath(joinpath(GLOBAL_FOLDER,output_folder_name))
    #origin_path = pwd()
    #cd(joinpath(GLOBAL_FOLDER,output_folder_name))
    file_links_list_to_fetch = []

    for url in file_links_list
        filename = URIs.splitpath(parse(URI, url))[3]
        local_path = joinpath(output_folder_name,filename)
        target_path = joinpath(GLOBAL_FOLDER,local_path)
        cache_path = get(path_mapping_dict,local_path,missing)
        if ismissing(cache_path)
            push!(file_links_list_to_fetch,url)
        else
            if !ispath(target_path)
                symlink(abspath(cache_path),target_path)
                @info "Using cached file: $filename for $STORE_NAME"
            end
        end
    end

    println("file_links_list_to_fetch length: ",length(file_links_list_to_fetch))

    # write to txt file
    open("urls.txt", "w") do io
        for url in file_links_list_to_fetch
            println(io, url)
            println(io, "   header= Cookie:$cftpsid_cookie")
            println(io, "   out=$GLOBAL_FOLDER/$output_folder_name/$(URIs.splitpath(parse(URI, url))[3])")
        end
    end
    # julia help run(), run with wait=true

    run(`aria2c --input-file urls.txt`)

end

end