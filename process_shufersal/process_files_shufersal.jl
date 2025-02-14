module ShufersalProcessFiles
export processFiles

#=
using DataFramesMeta
using Dates
using HTTP
using Inflate
using JSON
using GZip
using URIs
using ZipArchives
using CSV
=#
using LibDeflate
using EzXML
using DataFrames
using Serialization
using ProgressMeter


function promofull_file_to_dataframe(file_path::String, decompressor_object::Decompressor)::DataFrame
    decompressor_output = Vector{UInt8}()
    open(file_path) do io
        gzip_decompress!(decompressor_object, decompressor_output, read(io))
    end
    xml::EzXML.Document = parsexml(decompressor_output)
    decompressor_output = nothing

    item_nodes = findall("//Promotion", xml.root)
    chain_id = parse(Int64, findfirst("//ChainId", xml.root).content)
    store_id = parse(Int64, findfirst("//StoreId", xml.root).content)

    result = DataFrame()
    for item_node in item_nodes
        promotion_items = Vector{Int64}()
        row = Dict{String,Union{Int64,String,Vector{Int64}}}(
            "ChainId" => chain_id,
            "StoreId" => store_id,
            "PromotionItems" => promotion_items
        )
        for element in eachelement(item_node)
            if element.name in ["PromotionId", "AllowMultipleDiscounts", "RewardType", "MinNoOfItemOfered", "AdditionalIsCoupon", "AdditionalGiftCount", "AdditionalIsTotal", "AdditionalIsActive", "DiscountRate","DiscountType"]
                row[element.name] = parse(Int64, element.content)
            elseif element.name in ["PromotionDescription", "PromotionUpdateDate", "PromotionStartDate", "PromotionStartHour", "PromotionEndDate", "PromotionEndHour","DiscountedPrice","IsWeightedPromo","AdditionalRestrictions","Remark","MinQty","MaxQty","MinAmount","MinPurchaseAmnt","MaxAmount"]
                row[element.name] = element.content
            elseif element.name == "PromotionItems"
                # resize promotion_items
                resize!(promotion_items, parse(Int64,element["Count"]))
                # because the vector is now prepopulated with zeroes(0) this index tells us which zero to replace with the itemcode
                index_to_push_itemcodes_into = 1
                for subelement in eachelement(element)
                    if subelement.name == "Item"
                        for subsubelement in eachelement(subelement)
                            if subsubelement.name == "ItemCode"
                                # here we replace the entry in the preallocated array which is populated with zeroes by default
                                promotion_items[index_to_push_itemcodes_into] = parse(Int64, subsubelement.content)
                                index_to_push_itemcodes_into += 1
                            end
                        end
                    end
                end
            elseif element.name == "Clubs"
                for subelement in eachelement(element)
                    if subelement.name == "ClubId"
                        row["ClubId"] = parse(Int64, subelement.content)
                    end
                end
            else
                @warn "Unknown element in promos: $(element.name)"
            end
        end
        push!(result, row; promote=true,cols=:union)
    end
    return result
end

function pricefull_file_to_dataframe(file_path::String, decompressor_object::Decompressor)::DataFrame
    decompressor_output = Vector{UInt8}()
    open(file_path) do io
        gzip_decompress!(decompressor_object, decompressor_output, read(io))
    end
    xml::EzXML.Document = parsexml(decompressor_output)
    decompressor_output = nothing

    item_nodes = findall("//Item", xml.root)
    chain_id = parse(Int64, findfirst("//ChainId", xml.root).content)
    store_id = parse(Int64, findfirst("//StoreId", xml.root).content)

    result = DataFrame()
    for item_node in item_nodes
        row = Dict{String,Union{Int64,Float64,String}}(
            "ChainId" => chain_id,
            "StoreId" => store_id
        )
        for element in eachelement(item_node)
            if element.name in ["ItemCode", "QtyInPackage"]
                row[element.name] = parse(Int64, element.content)
            elseif element.name in ["Quantity", "ItemPrice", "UnitOfMeasurePrice"]
                row[element.name] = parse(Float64, element.content)
            elseif element.name in ["PriceUpdateDate", "ItemType", "ItemName", "ManufacturerName", "ManufactureCountry", "ManufacturerItemDescription", "UnitQty", "bIsWeighted", "UnitOfMeasure", "AllowDiscount", "ItemStatus"]
                row[element.name] = element.content
            else
                @warn "Unknown element in prices: $(element.name)"
            end
        end
        push!(result, row;promote=true,cols=:union)
    end
    return result
end

function processFiles(GLOBAL_FOLDER::String)
    STORE_NAME = "Shufersal"
    #GLOBAL_FOLDER = "scratch_folder"

    @info "Processing Shufersal files..."

    output_folder_name = "$STORE_NAME-output"
    file_name_list = readdir("$GLOBAL_FOLDER/$output_folder_name")

    promofull_filenames = filter(x->occursin("PromoFull",x),file_name_list)
    pricefull_filenames = filter(x->occursin("PriceFull",x),file_name_list)

    #
    # Process promotions
    #
    decompressor_object = Decompressor()
    promotions_table = DataFrame() 

    @showprogress desc="Promos" for file in promofull_filenames
        df = promofull_file_to_dataframe(joinpath("$GLOBAL_FOLDER/$output_folder_name", file),decompressor_object)
        append!(promotions_table, df; promote=true,cols=:union)
        df = nothing
    end

    serialize("$GLOBAL_FOLDER/$STORE_NAME-PromotionsTable.df",promotions_table)
    promotions_table = nothing

    #
    # Process prices
    #
    prices_table = DataFrame() 

    @showprogress desc="Prices" for file in pricefull_filenames
        df = pricefull_file_to_dataframe(joinpath("$GLOBAL_FOLDER/$output_folder_name", file),decompressor_object)
        append!(prices_table, df; promote=true,cols=:union)
        df = nothing
    end

    serialize("$GLOBAL_FOLDER/$STORE_NAME-PricesTable.df",prices_table)
end

end