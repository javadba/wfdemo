# KeywordsServlet #

## Build & Run ##

```sh
$ cd KeywordsServlet
$ ./sbt
> container:start
> browse
```

If `browse` doesn't launch your browser, manually open [http://localhost:8080/](http://localhost:8080/) in your browser.

To run the RegexFilters:

   sbt "run local[*] /shared/dataSmall 8 3 true src/main/resources/PosRegex.json src/main/resources/NegRegex.json interaction_created_at"


