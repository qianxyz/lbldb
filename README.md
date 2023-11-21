# Line-by-line Database

## Create

```python
# create new table
students = Database("./students.csv", fieldnames=["name", "age"])

# load existing table (fieldnames are inferred)
pokemons = Database("./example/pokemon.csv")

# add a row to a table
students.append({"name": "John", "age": 23})
```

## Read

```python
# list all pokemons
Query(pokemons).execute()

# filtering: find all pokemons taller than 100
Query(pokemons) \
    .filter(pokemons.height > 100) \
    .execute()

# projection: only list names for such pokemons
Query(pokemons) \
    .filter(pokemons.height > 100) \
    .project(pokemons.identifier) \
    .execute()

# (outer) join: initialize query with multiple dbs
# to get anything useful, use filtering afterwards

# find at which level does bulbasaur learn razor leaf in red/blue
Query(pokemons, moves, learns) \
    .filter(
        (pokemons.id == learns.pokemon_id) &
        (moves.id == learns.move_id)
    ) \
    .filter(pokemons.identifier == "bulbasaur") \
    .filter(moves.identifier == "razor-leaf") \
    .filter(learns.version_group_id == "1") \
    .project(learns.level) \
    .execute()

# groupby and aggregation: count numbers of moves for each generation
Query(moves) \
    .groupby(moves.generation_id) \
    .count()

# sorting: sort all pokemons by their identifier (dictionary order)
Query(pokemons) \
    .sort(pokemons.identifier) \
    .execute()
```
