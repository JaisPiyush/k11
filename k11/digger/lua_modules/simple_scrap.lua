function main(splash)
    assert(splash:go(splash.args.url))
    splash:wait(0.5)

    return {
        url=splash.args.url,
        html=splash:html()
    }
end