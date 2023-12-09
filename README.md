# Line-by-line Database

## Usage

The implementation is contained in the single `lbldb.py` file. 

The script `lbldb` provides the CLI entrance:

```bash
$ ./lbldb  # drop into REPL
$ ./lbldb < example/1_create  # or other examples in the directory
```

## Language Specification

NOTE: The following snippets are intended to showcase the language. To actually
run them, make sure all the tables are loaded beforehand. For some examples
that can be run directly, see the examples directory.

### Create

```python
# create new table
students = Database("./students.csv", fieldnames=["name", "age"])

# load existing table (fieldnames are inferred)
pokemons = Database("./example/pokemon.csv")

# add a row to a table
students.append({"name": "John", "age": 23})
```

### Read

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

### Update
```python
# set the age of John to 25
Update(students) \
    .filter(students.name == "John") \
    .set(students.age, 25) \
    .execute()
```

### Delete
```python
# delete all students under 22
Delete(students) \
    .filter(students.age <= 22) \
    .execute()
```
