# import hack
with open("lbldb.py", "rb") as source_file:
    code = compile(source_file.read(), "lbldb.py", "exec")
exec(code)

pokemons = Database("example/pokemons_rb.csv")
moves = Database("example/moves_rb.csv")
learns = Database("example/learns_rb.csv")

Query(pokemons, moves, learns) \
    .filter(
        (pokemons.id == learns.pokemon_id) &
        (moves.id == learns.move_id)
    ) \
    .filter(pokemons.identifier == "bulbasaur") \
    .filter(moves.identifier == "razor-leaf") \
    .filter(learns.version_group_id == "1") \
    .project(learns.level) \
    .limit(1) \
    .execute()
