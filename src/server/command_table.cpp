#include "server/command_table.h"

#include <cctype>

#include "base/log.h"
#include "base/strings.h"

namespace kv {

CommandTable::CommandTable() {
  RegisterStringFamily(this);
  RegisterGenericFamily(this);
  RegisterServerFamily(this);
}

void CommandTable::Add(CommandSpec spec) {
  KV_CHECK(by_name_.find(spec.name) == by_name_.end());
  KV_CHECK((spec.route == Route::kCoordinator) == (spec.coord_fn != nullptr));
  KV_CHECK((spec.route == Route::kCoordinator) == (spec.shard_fn == nullptr));
  specs_.push_back(std::make_unique<CommandSpec>(std::move(spec)));
  by_name_[specs_.back()->name] = specs_.back().get();
}

const CommandSpec* CommandTable::Find(std::string_view name) const {
  char lower[32];
  if (name.size() > sizeof(lower)) return nullptr;
  for (size_t i = 0; i < name.size(); ++i) {
    lower[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(name[i])));
  }
  // absl::string_view, not std::string_view: Debian's Abseil is built with its own type.
  auto it = by_name_.find(absl::string_view(lower, name.size()));
  return it == by_name_.end() ? nullptr : it->second;
}

bool CommandTable::ArityOk(const CommandSpec& cmd, size_t argc) {
  if (cmd.arity >= 0) return argc == static_cast<size_t>(cmd.arity);
  return argc >= static_cast<size_t>(-cmd.arity);
}

std::vector<const CommandSpec*> CommandTable::All() const {
  std::vector<const CommandSpec*> out;
  for (const auto& s : specs_) out.push_back(s.get());
  return out;
}

std::string WrongArgsError(std::string_view cmd) {
  return "ERR wrong number of arguments for '" + std::string(cmd) + "' command";
}

std::string InvalidExpireError(std::string_view cmd) {
  return "ERR invalid expire time in '" + std::string(cmd) + "' command";
}

// Same text as Redis 7: the name and up to ~128 bytes of arguments, each quoted.
std::string UnknownCommandError(std::span<const std::string_view> args) {
  std::string out = "ERR unknown command '";
  out.append(args[0].substr(0, 128));
  out.append("', with args beginning with: ");
  std::string list;
  for (size_t i = 1; i < args.size() && list.size() < 128; ++i) {
    list += '\'';
    list.append(args[i].substr(0, 128 - list.size()));
    list += "' ";
  }
  out += list;
  return out;
}

std::string UnknownSubcommandError(std::string_view sub, std::string_view cmd) {
  std::string upper;
  for (char c : cmd) upper.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  return "ERR unknown subcommand '" + std::string(sub.substr(0, 128)) + "'. Try " + upper + " HELP.";
}

}  // namespace kv
