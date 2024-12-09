# RevoData's Technical Assessment

## Introduction

This project will let you showcase your technical skills in data engineering, applied to a real-world dataset.

Let's imagine you're a very successful (and rich) data engineer who wants to reinvest your life's earnings in Amsterdam property, for a passive source of income. You are looking to buy houses and apartments, then either rent them out long-term (through Kamernet) or short-term (through Airbnb).

Skilled data engineer that you are, you have already extracted a dataset on house location and per-night prices from Airbnb, and you have a second data stream coming in from Kamernet, which you are actively scraping.

### Goal of the Assessment

You want to have an idea of which postal codes are better suited for investment, and from those, in which it's more profitable to rent long term or through Airbnb.

## Product Development

If you are applying for a non-engineering position:

- Create a conceptual design of how the implementation looks like (either components or process)
- Create a business case
- Create a roadmap with milestones how you would deliver this implementation with a team of engineers
- Create a storyline based on the above input to convince the customer to invest

## Engineering Deliverables

If you are applying for an engineering position, at a minimum, you will need to build a (set of) data pipeline(s) that:

- Ingest the rental data scraped from Kamernet: `./data/rentals.json`
- Ingest the data from Airbnb: `./data/airbnb.csv`
- Clean both datasets
- Calculate the potential average revenue per house, per postcode (rental and Airbnb)
- Follow the principles of the [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture#:~:text=A%20medallion%20architecture%20is%20a%20data%20design%20pattern,%28from%20Bronze%20%E2%87%92%20Silver%20%E2%87%92%20Gold%20layer%20tables%29.)

It is expected that you deliver as part of the project:

- Exploratory notebooks with data checks (in the `./scratch` folder)
- Notebook(s) for your pipeline (in the `./notebooks` folder)
- Libraries or (buildable) packages built for your pipeline (in the `./src/<package_name>` folder)
- Unit tests for your pipeline (in the `./tests` folder)
- Documentation for your pipeline (in the `README` and the `./docs` folder) - **explain the why, not the how**
- An export of the datasets produced by your pipeline, (use Parquet format, in the `./data/output` folder)
- Configurations for your pipeline jobs (in the `./resources` folder)

We highly recommend using Databricks, you can set up a [free trial for professional use following Express Setup](http://signup.databricks.com/). Note that the Community Edition does not provide all the functionality required for this assignment. However, most of all we are looking to understand how you work, so feel free to pick a tool you are most comfortable with - whether it's something like a local PySpark instance, DuckDB or a cloud service. Explain your reasoning.

Save everything in a private Git repository and share it with us. Deliver us a clean repository: remove any redundant files, replace our `README` with your own, provide instructions how to build and run your project. If you are unsure how to structure your repository we recommend using our own [RevoData Asset Bundle Templates](https://github.com/revodatanl/revo-asset-bundle-templates) as a starting point.

We expect you to spend 2-3 hours on the assessment, so apply your best judgment when prioritizing tasks.

### Stretch Goals

Following are a number of stretch goals of increasing difficulty that will give us an idea of how far you can go. We **do not expect** that you'll be able to achieve all of these in the given time, so pick and choose whatever suits you best. It's preferable to focus on a complete and high-quality initial assessment rather than getting lost achieving these goals.

#### Level 1 / Engineer

- [ ] Build a CI/CD pipeline that deploys your data pipeline
- [ ] Run tests in your CI/CD pipeline
- [ ] Use pre-commit hooks to ensure code quality

#### Level 2 / Artist

- [ ] Build a visualization or dashboard showing the potential revenue per postcode (rental and Airbnb)
- [ ] Create diagrams of the data flows and of your CI/CD pipeline

> **Please note:** for the following sections, you **will** need Databricks. Delta Live Tables (DLT) is not available in Databricks Community Edition, so you should use the free trial if you got this far. However, be aware that the free trial comes with capacity limitations that may impact your ability to complete the goals.
>
> _Continue at your own risk._

### BONUS

#### Level 3 / Future-Proof

- [ ] Use Delta Live Tables (DLT) to build your pipelines
- [ ] Use expectations (if using DLT) or another framework (if not), to ensure data quality
- [ ] Deploy your pipeline using Databricks Asset Bundles

#### Level 4 / Steampunk IED

- [ ] Load the data from `rentals.json` one record at a time with streaming ingestion
- [ ] Update the gold layer table(s) in real time as new streaming data arrives

#### Level 5 / Powerlevel 10K

- [ ] Use the `./data/geo/post_codes.geojson` geographic dataset to enrich the Airbnb data with missing postcodes
- [ ] - or - Query an external API such as [public.opendatasoft.com](https://public.opendatasoft.com/explore/dataset/georef-netherlands-postcode-pc4/api/) to fill in the missing postcodes using a UDF
- [ ] Use the `./data/geo/amsterdam_areas.geojson` geographic dataset for your visualization

## Review

Once you have completed this project, we shall review it together. We are paying special attention to the pipeline logic you followed, and to what you did when things went south. And with this data we're giving you, things will go pretty south, so be creative with your workarounds and maybe even move the goalposts a bit in your favour.

Finally, as a side-note, we're also using ChatGPT. It's great. So don't be shy about employing it if you can, we want to see that you've also internalized whatever lessons you've learned from its input.

**Good luck, and see you on the other side!**
