# Revealing secrets in sparql session level

This repo presents the code for the ISWC 2020 publication: Revealing secrets in sparql session level.

`Sec3-1.ipynb` and `Sec3-2.ipynb` present how we use this code to generate the results of Section 3.1 and Section 3.2 shown in our paper.

But we are not allowed to share the original SPARQL log because it contains sensitive IP addresses of users.

However, using LSQ 2.0 (http://www.semantic-web-journal.net/system/files/swj2866.pdf) directly is also a good choice, in which `lsqv:hostHash` is used to identify different users.

If you use our code or find our paper is useful, please consider cite our paper:
```
@inproceedings{zhang2020revealing,
  title={Revealing secrets in sparql session level},
  author={Zhang, Xinyue and Wang, Meng and Saleem, Muhammad and Ngomo, Axel-Cyrille Ngonga and Qi, Guilin and Wang, Haofen},
  booktitle={International Semantic Web Conference},
  pages={672--690},
  year={2020},
  organization={Springer}
}
```