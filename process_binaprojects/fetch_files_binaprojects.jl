module BinaprojectsFetchFiles
export fetchFiles

#=
using EzXML
using DataFrames
using DataFramesMeta
using Dates
using Inflate
using GZip
using ZipArchives
using CSV
using LibDeflate
=#

using HTTP
using JSON
using Serialization
using URIs



function fetchFiles(STORE_NAME::String,GLOBAL_FOLDER::String,path_mapping_dict::Dict)

    # Fetch file entries from binaprojects
    resp = HTTP.get("https://$STORE_NAME.binaprojects.com/MainIO_Hok.aspx")
    body_file_entries = JSON.parse(String(resp.body))

    file_links_list = map(x -> "https://$STORE_NAME.binaprojects.com/Download/$(x["FileNm"])",body_file_entries)

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
        filename = URIs.splitpath(parse(URI, url))[2]
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
            println(io, "   out=$GLOBAL_FOLDER/$output_folder_name/$(URIs.splitpath(parse(URI, url))[2])")
        end
    end
    # julia help run(), run with wait=true

    run(`aria2c --input-file urls.txt`)

end

end