

from fpy_datareader.estat import estatReader

def DataReader(
    name,
    data_source=None,
    start=None,
    end=None,
    retry_count=3,
    pause=0.1,
    session=None,
    api_key=None,
):

    expected_source = [
        "estat"
    ]

    if data_source not in expected_source:
        msg = "data_source=%r is not implemented" % data_source
        raise NotImplementedError(msg)

    if data_source == "estat":
        return estatReader(
            symbols=name,
            start=start,
            end=end,
            adjust_price=False,
            chunksize=25,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key
        ).read()

    else:
        msg = "data_source=%r is not implemented" % data_source
        raise NotImplementedError(msg)

