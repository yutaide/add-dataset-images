local cc = require("cc")
local u = require("util")
local env = require("env")
local cti = require("cti")
local file = require("file")
local original_data = require("original-data")
local dataset = require("dataset")
local thumbnails = require("thumbnails")
local uuid = require("uuid")
uuid.seed()

local prefix = env.cti_data_dir()
local file_tag_id = 1000 -- training

-- 特定のディレクトリ配下に追加したい画像群が格納されていること
local src_dir_path = "/product/worker/assets/images"
local dst_original_dir_path = "files/original/sample"
local dst_dataset_dir_path = "files/dataset/20241016080630-sample"


local function abs_path(path)
  return u.join_path(prefix, path)
end

local function exec_fn_except_dotfiles(v, fn)
  -- Skip dotfile such as .DS_Store
  if not v:match("^%.") then
    fn()
  end
end

local function add_original_data()
  local src_file_paths = {}
  local dst_file_paths = {}
  local res, err = cc.fs.list_files(src_dir_path)
  if res then
    src_file_paths = u.list_append(src_file_paths, u.map(res, function(f)
      return {path = u.join_path(src_dir_path, f)}
    end))
  end
  for _, src_file_path in pairs(src_file_paths) do
    exec_fn_except_dotfiles(u.basename(src_file_path.path), function()
      local dst_file_path = u.join_path(dst_original_dir_path, u.basename(src_file_path.path))
      local res, err = cc.fs.copy(src_file_path.path, abs_path(dst_file_path))
      cti.logger.debug("res of copy: ", res or err)
      if not res or err then
        -- Rename file and retry
        local basename = u.basename(src_file_path.path)
        local ext = u.extension(src_file_path.path)
        dst_file_path = u.join_path(dst_original_dir_path, basename:sub(1, #basename - #ext) .. "_" .. uuid() .. ext)
        res, err = cc.fs.copy(src_file_path.path, abs_path(dst_file_path))
        cti.logger.debug("[Retry]res of copy: ", res or err)
      end
      kDbHandler:insert({
        files = {
          values = {path = dst_file_path}
        }
      })
      kDbHandler:insert({
        map_files_file_tags = {
          values = {
            file_path = dst_file_path,
            file_tag_id = file_tag_id
          }
        }
      })
      table.insert(dst_file_paths, dst_file_path)
      -- Create thumbnail
      local ok, err = thumbnails.create_thumb(dst_file_path, {rate = 0.5})
    end)
  end
  return dst_file_paths
end

local function add_dataset_data(original_datas)
  for _, original_data in pairs(original_datas) do
    local dst_file_path = u.join_path(dst_dataset_dir_path, u.basename(original_data))
    local res, err = cc.fs.copy(abs_path(original_data), abs_path(dst_file_path))
    cti.logger.debug("res of copy: ", res or err)
    kDbHandler:insert({
      files = {
        values = {
          path = dst_file_path
        }
      }
    })
    kDbHandler:insert({
      map_files_file_tags = {
        values = {
          file_path = dst_file_path,
          file_tag_id = file_tag_id
        }
      }
    })
    -- Create thumbnail
    local ok, err = thumbnails.create_thumb(dst_file_path, {rate = 0.5})
  end
  return true
end

local function proc()
  local original_datas = add_original_data()
  return add_dataset_data(original_datas)
end

return {3, proc}
