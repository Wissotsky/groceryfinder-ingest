module ShufersalFetchFiles
export fetchFiles

#=
using DataFrames
using DataFramesMeta
using Dates
using Inflate
using JSON
using GZip
using ZipArchives
using CSV
using LibDeflate
=#

using HTTP
using EzXML
using Serialization
using URIs



function fetchFiles(GLOBAL_FOLDER::String)
    STORE_NAME = "Shufersal"
    #GLOBAL_FOLDER = "scratch_folder"

    @info "Fetching Shufersal files..."

    # fetch store list
    resp = HTTP.get("https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=0&storeId=0")
    htmlstring = String(resp.body)
    htmlparsed = parsehtml(htmlstring)

    url = findlast("//a[contains(@href, 'page')]", htmlparsed.root)["href"]
    parsed_url = parse(URI, url)
    max_page_number = parse(Int64,queryparams(parsed_url)["page"])
    page_list = ["https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=0&storeId=0&page=$(i)" for i in 1:max_page_number]

    function get_file_links_from_page(url::String)::Vector{String}
        resp = HTTP.get(url)
        htmlstring = String(resp.body)
        htmlparsed = parsehtml(htmlstring)
        urls = getindex.(findall("//a[contains(@href, 'pricesprodpublic')]", htmlparsed.root),"href")
        return urls
    end

    file_links_list = vcat(asyncmap(get_file_links_from_page, page_list)...)

    # load StoresDataframe from serialized
    StoresDataframe = deserialize("$GLOBAL_FOLDER/$STORE_NAME-StoresTable.df")

    chainIdLength = 13 # TODO: hardcoded

    # list of store ids that we want to fetch
    allowed_store_ids = [div(storeId,10^chainIdLength) for storeId in StoresDataframe.StoreIds]

    # list of pricefull and promofull files
    filter!(x -> occursin(r"pricefull",x) || occursin(r"promofull",x),file_links_list)

    # list of store urls that we want to fetch
    filter!(x -> in(parse(Int64,match(r"-(\d*)-",x).captures[1]),allowed_store_ids),file_links_list)

    output_folder_name = "$STORE_NAME-output"
    # write to txt file
    open("urls.txt", "w") do io
        for url in file_links_list
            println(io, url)
            println(io, "   out=$GLOBAL_FOLDER/$output_folder_name/$(URIs.splitpath(parse(URI, url))[2])")
        end
    end
    # julia help run(), run with wait=true
    # .\aria2c.exe --input-file urls.txt

    run(`aria2c --input-file urls.txt`)

end

end