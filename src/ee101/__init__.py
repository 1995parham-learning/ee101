import datetime
import logging
import typing

import ee

logger = logging.getLogger(__name__)


# google earth sdk doesn't have any typing, we need to ignore it completely.
@typing.no_type_check
def get_arable_acres(
    _start_date: datetime.date,
    _end_date: datetime.date,
    geom: GEOSGeometry,
):
    start_date = _start_date.isoformat()
    end_date = _end_date.isoformat()

    logger.info(f"arability request from {start_date} to {end_date}")
    geometry = ee.Geometry(
        json.loads(geom.json),
        # "EPSG:4326",
    )

    dw = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filter(ee.Filter.date(start_date, end_date))
        .filter(ee.Filter.bounds(geometry))
    )
    # gather image collection metadata
    #
    # retrieve the count of images in the collection.
    count = dw.size().getInfo()

    # obtain the date range of the images in the collection.
    date_range = dw.reduceColumns(ee.Reducer.minMax(), ["system:time_start"])

    logger.info(
        "there are %d images available on: %s - %s",
        count,
        ee.Date(date_range.get("min")).format().getInfo(),
        ee.Date(date_range.get("max")).format().getInfo(),
    )

    # the snow pixels (with a value of 8) are being masked out
    # as they do not contribute to predicting the land use.
    def mask_snow(image):
        condition = image.select("label").lt(8)
        # return the image with the mask applied
        return image.updateMask(condition)

    dw = dw.map(mask_snow)

    # select "label" band which shows the land-use
    # that has more probability
    classification = dw.select("label")
    # this will compute mode on image collection and return one image
    # https://www.geeksforgeeks.org/difference-between-mean-median-and-mode-with-examples/
    dw_composite = (
        classification.sort("system:time_start", False)
        .reduce(ee.Reducer.mode())
        .rename(["classification"])
    )

    decimal_format = f"%.{8}f"

    dataset = ee.Image.pixelArea().divide(self.denominator).addBands(dw_composite)

    init_result = dataset.reduceRegions(
        collection=geometry,
        reducer=ee.Reducer.sum().group(
            groupField=1,
            groupName="group",
        ),
        # bestEffort=True,
        # maxPixels=1e24,
        crs="EPSG:4326",
        scale=10,
    )

    groups = ee.List(init_result.first().get("groups"))
    # logger.info(f"land used: {groups.getInfo()}")
    keys = groups.map(lambda x: ee.Number(ee.Dictionary(x).get("group")))
    values = groups.map(
        lambda x: ee.Number.parse(
            ee.Number(ee.Dictionary(x).get("sum")).format(decimal_format)
        )
    )
    # logger.info(f"total area of land in m^2: {total_area.getInfo()}")

    arability = arability_type()

    keys = keys.getInfo()
    values = values.getInfo()

    total_area = sum(values)

    if total_area > 0:
        for name, index in self.indexes.items():
            value = values[keys.index(index)] if index in keys else 0
            fraction = (value / total_area) * 100

            setattr(arability, name, round(fraction, 2))

    return arability
