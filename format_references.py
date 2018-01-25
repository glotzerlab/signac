from pybtex import format_from_file

with open('references.bib') as bib_file:
    s_formatted = format_from_file(
        bib_file,
        style='unsrt',
        abbreviate_names=True,
        output_backend='text',
        capfirst=False,
        )
    print(s_formatted.replace('Csadorf', 'csadorf'))
